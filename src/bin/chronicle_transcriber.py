#!/usr/bin/env python3
"""
Chronicle Transcriber — Persistent Whisper daemon for AGX.

Watches /tmp/ear_inbox/ for WAV files from the Pi's chronicle_ear.py,
transcribes them with a persistent faster-whisper model, and writes
results to Mind's scratch_pad as category='heard-speech'.

Runs on: AGX Orin (nate-agx@192.168.1.70)
Depends on: faster-whisper, sqlite3

Architecture:
  Pi (chronicle_ear.py) → SCP WAV to /tmp/ear_inbox/
  AGX (this) → inotify/poll → Whisper small → scratch_pad → Mind reads next cycle
"""

import os
import sys
import time
import glob
import json
import signal
import sqlite3
import logging

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INBOX_DIR = "/tmp/ear_inbox"
DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
WHISPER_MODEL = "small"
WHISPER_DEVICE = "cpu"          # Will switch to "cuda" once ctranslate2 CUDA is built
WHISPER_COMPUTE = "float32"     # int8 crashes on aarch64 (ctranslate2 bug); float16 for CUDA
WHISPER_BEAM_SIZE = 3

# Hallucination filter — Whisper's common false positives on silence/noise
HALLUCINATIONS = {
    "", ".", "..", "...", "Thank you.", "Thanks for watching!",
    "Thanks for watching.", "Thank you for watching.",
    "Please subscribe.", "Bye.", "Bye-bye.", "Goodbye.",
    "The end.", "you", "You", "I'm sorry.",
    "Subtitles by the Amara.org community",
    "ご視聴ありがとうございました",
}

# Rate limiting
MAX_SEGMENTS_PER_MINUTE = 10    # Safety valve
MIN_SEGMENT_INTERVAL_S = 1.0   # Don't process faster than this

# Scratch pad
SCRATCH_CATEGORY = "heard-speech"
SCRATCH_PRIORITY = 7            # High enough to surface in Mind's prompt

# Logging
LOG_FILE = "/tmp/chronicle_transcriber.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRANSCRIBER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def write_to_scratch_pad(db_path: str, text: str, duration_s: float):
    """Write transcribed speech to Mind's scratch_pad."""
    ts = int(time.time())
    content = json.dumps({
        "text": text,
        "duration_s": round(duration_s, 1),
        "timestamp": ts,
        "source": "chronicle-ear",
    })
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        conn.execute(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, ?, ?)",
            (content, SCRATCH_CATEGORY, SCRATCH_PRIORITY, ts, ts),
        )
        conn.commit()
        conn.close()
        log.info(f"  Written to scratch_pad: {text[:80]}")
        return True
    except Exception as e:
        log.error(f"  DB write failed: {e}")
        return False


def auto_resolve_old_speech(db_path: str, max_age_s: int = 600):
    """Auto-resolve heard-speech notes older than max_age_s."""
    try:
        cutoff = int(time.time()) - max_age_s
        conn = sqlite3.connect(db_path, timeout=5)
        cur = conn.execute(
            "UPDATE scratch_pad SET resolved=1 WHERE category=? AND resolved=0 AND created_at < ?",
            (SCRATCH_CATEGORY, cutoff),
        )
        if cur.rowcount > 0:
            log.info(f"  Auto-resolved {cur.rowcount} old speech notes")
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"  Auto-resolve failed: {e}")


# ---------------------------------------------------------------------------
# Transcription
# ---------------------------------------------------------------------------
def load_whisper():
    """Load faster-whisper model (persistent, loaded once)."""
    from faster_whisper import WhisperModel
    log.info(f"Loading Whisper {WHISPER_MODEL} (device={WHISPER_DEVICE}, compute={WHISPER_COMPUTE})...")
    start = time.time()
    model = WhisperModel(WHISPER_MODEL, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE)
    elapsed = time.time() - start
    log.info(f"  Whisper loaded in {elapsed:.1f}s")
    return model


def transcribe(model, wav_path: str) -> tuple:
    """Transcribe a WAV file. Returns (text, duration_s, elapsed_s)."""
    import wave
    # Get duration from WAV header
    try:
        with wave.open(wav_path, "r") as wf:
            frames = wf.getnframes()
            rate = wf.getframerate()
            duration_s = frames / rate
    except Exception:
        duration_s = 0.0

    start = time.time()
    segments, info = model.transcribe(
        wav_path,
        beam_size=WHISPER_BEAM_SIZE,
        language="en",             # Force English — avoids false Japanese/Chinese on short clips
        vad_filter=False,          # VAD already done on Pi side; ctranslate2 VAD crashes on aarch64
    )
    text = " ".join(seg.text.strip() for seg in segments).strip()
    elapsed = time.time() - start

    return text, duration_s, elapsed


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    log.info("Chronicle Transcriber starting...")
    log.info(f"  Inbox: {INBOX_DIR}")
    log.info(f"  Database: {DB_PATH}")
    log.info(f"  Model: {WHISPER_MODEL} ({WHISPER_DEVICE}/{WHISPER_COMPUTE})")

    # Ensure inbox exists
    os.makedirs(INBOX_DIR, exist_ok=True)

    # Load Whisper once
    model = load_whisper()

    # Stats
    total_transcribed = 0
    total_speech_found = 0
    last_process_time = 0

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log.info("  Watching for audio segments...")

    while running:
        try:
            # Auto-resolve old speech notes (>10 min)
            auto_resolve_old_speech(DB_PATH)

            # Check for WAV files in inbox
            wav_files = sorted(glob.glob(os.path.join(INBOX_DIR, "*.wav")))

            if not wav_files:
                time.sleep(0.5)
                continue

            # Rate limiting
            now = time.time()
            if now - last_process_time < MIN_SEGMENT_INTERVAL_S:
                time.sleep(MIN_SEGMENT_INTERVAL_S - (now - last_process_time))

            for wav_path in wav_files[:MAX_SEGMENTS_PER_MINUTE]:
                if not running:
                    break

                basename = os.path.basename(wav_path)
                log.info(f"Processing: {basename}")

                try:
                    text, duration_s, elapsed_s = transcribe(model, wav_path)
                    total_transcribed += 1
                    last_process_time = time.time()

                    log.info(f"  Transcribed ({duration_s:.1f}s audio in {elapsed_s:.1f}s): "
                             f"{text[:100] if text else '(silence)'}")

                    # Filter hallucinations
                    if text in HALLUCINATIONS or len(text) < 2:
                        log.info(f"  Filtered (hallucination or too short)")
                    else:
                        total_speech_found += 1
                        write_to_scratch_pad(DB_PATH, text, duration_s)

                except Exception as e:
                    log.error(f"  Transcription error: {e}")

                # Remove processed file
                try:
                    os.remove(wav_path)
                except OSError:
                    pass

        except Exception as e:
            log.error(f"Error in main loop: {e}")
            time.sleep(2)

    log.info(f"Chronicle Transcriber stopped. "
             f"Transcribed: {total_transcribed}, Speech found: {total_speech_found}")


if __name__ == "__main__":
    main()
