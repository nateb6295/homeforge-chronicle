#!/usr/bin/env python3
"""
Chronicle Ear — Always-listening VAD daemon for Raspberry Pi.

Continuously captures audio from the USB microphone, runs Silero VAD
to detect speech segments, and ships them to AGX for transcription.

Runs on: Raspberry Pi 5 (nathaniel@192.168.1.10)
Depends on: onnxruntime, numpy, wave (stdlib)

Architecture:
  Pi (this) → continuous arecord → ring buffer → Silero VAD → speech WAV → SCP to AGX
  AGX (chronicle_transcriber.py) → watches /tmp/ear_inbox/ → Whisper → scratch_pad
"""

import os
import sys
import time
import wave
import struct
import subprocess
import signal
import logging
import tempfile
import numpy as np

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SAMPLE_RATE = 16000
CHANNELS = 1
SAMPLE_WIDTH = 2  # 16-bit
ALSA_DEVICE = "plughw:2,0"

# VAD settings
VAD_THRESHOLD = 0.4           # Silero confidence threshold for speech
SPEECH_PAD_MS = 400           # Pad speech segments with this much silence
MIN_SPEECH_MS = 500           # Ignore segments shorter than this
MAX_SPEECH_S = 30             # Cap speech segments at 30 seconds
SILENCE_TIMEOUT_S = 1.5       # End speech after this much silence

# Ring buffer: 60 seconds of audio at 16kHz
RING_BUFFER_S = 60
CHUNK_MS = 32                 # VAD processes 32ms chunks (512 samples at 16kHz)
CHUNK_SAMPLES = int(SAMPLE_RATE * CHUNK_MS / 1000)  # = 512

# AGX connection
AGX_USER = "nate-agx"
AGX_HOST = "192.168.1.70"
AGX_INBOX = "/tmp/ear_inbox"

# Mute detection: don't capture while Mind is speaking
MUTE_FILE = "/tmp/chronicle_speaking"

# Logging
LOG_FILE = "/tmp/chronicle_ear.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EAR] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Silero VAD
# ---------------------------------------------------------------------------
class SileroVAD:
    """Lightweight Silero VAD wrapper using ONNX Runtime."""

    def __init__(self, threshold=VAD_THRESHOLD):
        import onnxruntime as ort

        # Find Silero VAD model — check common locations
        model_paths = [
            os.path.expanduser("~/.cache/silero-vad/silero_vad.onnx"),
            "/tmp/silero_vad.onnx",
        ]
        model_path = None
        for p in model_paths:
            if os.path.exists(p):
                model_path = p
                break

        if model_path is None:
            # Download Silero VAD ONNX model from HuggingFace
            model_path = model_paths[0]
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            log.info("Downloading Silero VAD model...")
            import urllib.request
            url = "https://huggingface.co/onnx-community/silero-vad/resolve/main/onnx/model.onnx?download=true"
            urllib.request.urlretrieve(url, model_path)
            log.info(f"Downloaded to {model_path}")

        self.session = ort.InferenceSession(model_path)
        self.threshold = threshold
        # State shape: (2, batch=1, 128) — combined h/c for this model version
        self._state = np.zeros((2, 1, 128), dtype=np.float32)
        self._sr = np.array(SAMPLE_RATE, dtype=np.int64)

    def reset(self):
        """Reset hidden state between utterances."""
        self._state = np.zeros((2, 1, 128), dtype=np.float32)

    def is_speech(self, audio_chunk: np.ndarray) -> float:
        """Run VAD on a chunk. Returns speech probability 0.0-1.0."""
        if len(audio_chunk) != CHUNK_SAMPLES:
            return 0.0

        # Normalize int16 to float32 [-1, 1]
        audio_f32 = audio_chunk.astype(np.float32) / 32768.0
        audio_f32 = audio_f32.reshape(1, -1)

        ort_inputs = {
            "input": audio_f32,
            "state": self._state,
            "sr": self._sr,
        }
        ort_outputs = self.session.run(None, ort_inputs)
        prob = ort_outputs[0].item()
        self._state = ort_outputs[1]
        return prob


# ---------------------------------------------------------------------------
# Audio capture
# ---------------------------------------------------------------------------
def start_arecord():
    """Start arecord process, return Popen object."""
    cmd = [
        "arecord",
        "-D", ALSA_DEVICE,
        "-f", "S16_LE",
        "-r", str(SAMPLE_RATE),
        "-c", str(CHANNELS),
        "-t", "raw",
        "-q",  # quiet
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return proc


def save_wav(samples: np.ndarray, path: str):
    """Save int16 samples as WAV file."""
    with wave.open(path, "w") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(samples.tobytes())


def ship_to_agx(wav_path: str) -> bool:
    """SCP a WAV file to AGX's ear inbox."""
    try:
        # Ensure inbox exists on AGX
        subprocess.run(
            ["ssh", "-o", "ConnectTimeout=3", "-o", "StrictHostKeyChecking=no",
             f"{AGX_USER}@{AGX_HOST}", f"mkdir -p {AGX_INBOX}"],
            capture_output=True, timeout=5,
        )
        # SCP the file
        basename = os.path.basename(wav_path)
        r = subprocess.run(
            ["scp", "-o", "ConnectTimeout=3", "-o", "StrictHostKeyChecking=no",
             wav_path, f"{AGX_USER}@{AGX_HOST}:{AGX_INBOX}/{basename}"],
            capture_output=True, text=True, timeout=15,
        )
        return r.returncode == 0
    except Exception as e:
        log.error(f"Ship failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    log.info("Chronicle Ear starting...")
    log.info(f"  Device: {ALSA_DEVICE}")
    log.info(f"  Sample rate: {SAMPLE_RATE}")
    log.info(f"  VAD threshold: {VAD_THRESHOLD}")
    log.info(f"  AGX: {AGX_USER}@{AGX_HOST}:{AGX_INBOX}")

    # Initialize VAD
    vad = SileroVAD(threshold=VAD_THRESHOLD)
    log.info("  Silero VAD loaded")

    # Ensure AGX inbox exists
    subprocess.run(
        ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
         f"{AGX_USER}@{AGX_HOST}", f"mkdir -p {AGX_INBOX}"],
        capture_output=True, timeout=10,
    )

    # Track stats
    segments_shipped = 0
    last_speech_time = 0

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    while running:
        try:
            # Start audio capture
            proc = start_arecord()
            log.info("  Audio capture started")

            # State machine: IDLE → SPEECH → TRAILING_SILENCE → IDLE
            state = "IDLE"
            speech_buffer = []
            silence_chunks = 0
            speech_start = 0
            pad_chunks = int(SPEECH_PAD_MS / CHUNK_MS)
            silence_timeout_chunks = int(SILENCE_TIMEOUT_S * 1000 / CHUNK_MS)
            max_speech_chunks = int(MAX_SPEECH_S * 1000 / CHUNK_MS)
            pre_speech_buffer = []  # Keep last N chunks for padding

            while running:
                # Read a chunk of raw audio
                raw = proc.stdout.read(CHUNK_SAMPLES * SAMPLE_WIDTH)
                if len(raw) < CHUNK_SAMPLES * SAMPLE_WIDTH:
                    log.warning("arecord underrun, restarting")
                    break

                chunk = np.frombuffer(raw, dtype=np.int16)

                # Check mute (Mind is speaking)
                if os.path.exists(MUTE_FILE):
                    if state == "SPEECH":
                        log.info("  Muted while speaking — discarding segment")
                        speech_buffer = []
                        state = "IDLE"
                        vad.reset()
                    continue

                # Run VAD
                prob = vad.is_speech(chunk)

                if state == "IDLE":
                    # Keep a pre-speech buffer for padding
                    pre_speech_buffer.append(chunk)
                    if len(pre_speech_buffer) > pad_chunks:
                        pre_speech_buffer.pop(0)

                    if prob >= VAD_THRESHOLD:
                        state = "SPEECH"
                        speech_start = time.time()
                        speech_buffer = list(pre_speech_buffer)  # include pre-speech padding
                        speech_buffer.append(chunk)
                        silence_chunks = 0
                        log.info(f"  Speech detected (prob={prob:.2f})")

                elif state == "SPEECH":
                    speech_buffer.append(chunk)

                    if prob < VAD_THRESHOLD:
                        silence_chunks += 1
                    else:
                        silence_chunks = 0

                    # End conditions
                    speech_too_long = len(speech_buffer) >= max_speech_chunks
                    silence_ended = silence_chunks >= silence_timeout_chunks

                    if silence_ended or speech_too_long:
                        # Compute duration
                        duration_s = len(speech_buffer) * CHUNK_MS / 1000
                        duration_ms = int(duration_s * 1000)

                        if duration_ms < MIN_SPEECH_MS:
                            log.info(f"  Segment too short ({duration_ms}ms), discarding")
                        else:
                            # Concatenate and save
                            audio = np.concatenate(speech_buffer)
                            ts = int(time.time())
                            wav_path = f"/tmp/ear_segment_{ts}_{duration_ms}ms.wav"
                            save_wav(audio, wav_path)
                            log.info(f"  Speech segment: {duration_s:.1f}s → {wav_path}")

                            # Ship to AGX
                            if ship_to_agx(wav_path):
                                segments_shipped += 1
                                last_speech_time = time.time()
                                log.info(f"  Shipped to AGX (total: {segments_shipped})")
                            else:
                                log.error(f"  Failed to ship to AGX")

                            # Clean up local file
                            try:
                                os.remove(wav_path)
                            except OSError:
                                pass

                        # Reset state
                        speech_buffer = []
                        pre_speech_buffer = []
                        silence_chunks = 0
                        state = "IDLE"
                        vad.reset()

            # Clean up arecord
            proc.terminate()
            proc.wait(timeout=5)

        except Exception as e:
            log.error(f"Error in main loop: {e}")
            time.sleep(2)

    log.info(f"Chronicle Ear stopped. Shipped {segments_shipped} segments total.")


if __name__ == "__main__":
    main()
