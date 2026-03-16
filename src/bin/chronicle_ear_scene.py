#!/usr/bin/env python3
"""
Chronicle Ear Scene — Audio scene classification for Raspberry Pi 5.

Captures short audio samples every 30 seconds (via arecord, non-continuous
to avoid ALSA conflicts with chronicle_ear.py), analyzes the spectral content
to classify the ambient sound scene, and publishes results to MQTT.

Runs on: Raspberry Pi 5 (nathaniel@192.168.1.10)
Depends on: numpy (already installed for chronicle_ear.py)
Optional:  paho-mqtt (pip3 install paho-mqtt)

Architecture:
  arecord (10s burst) -> numpy FFT analysis -> scene classification -> MQTT
  MQTT topic: homeforge/home/ear/scene

Spectral approach (no model download needed):
  - Music:      sustained pitched content, broad frequency spread, periodicity
  - Speech:     energy concentrated in 300-3000Hz (formant region), modulated
  - Silence:    low overall energy
  - Percussive: sudden energy spikes (dishes, doors, claps)
  - Mechanical: steady narrow-band energy (fans, HVAC, hum)
  - Nature:     broadband noise with no strong peaks (rain, wind)
  - TV/Media:   mix of speech + music + compressed dynamic range

YAMNet upgrade path documented at bottom of file.
"""

import os
import sys
import time
import json
import struct
import subprocess
import signal
import logging
import tempfile
from collections import deque
from typing import Dict, List, Tuple, Optional

import numpy as np

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SAMPLE_RATE = 16000
CHANNELS = 1
SAMPLE_WIDTH = 2           # 16-bit signed LE
ALSA_DEVICE = "plughw:2,0"

CAPTURE_DURATION_S = 10    # Seconds of audio per analysis window
CYCLE_INTERVAL_S = 30      # Seconds between captures

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "homeforge/home/ear/scene"

# Also write latest scene for ambient_collector to pick up
SCENE_FILE = "/tmp/chronicle_ear_scene.json"

# Silence threshold — RMS below this (in int16 scale) is silence.
# Typical quiet room mic noise floor is ~100-300 on int16 scale.
SILENCE_RMS_THRESHOLD = 400

# Number of past results to keep for smoothing / trend detection
HISTORY_LEN = 5

LOG_FILE = "/tmp/chronicle_ear_scene.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCENE] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Audio capture (burst mode — no ALSA conflict with ear.py)
# ---------------------------------------------------------------------------
def capture_audio(duration_s: int = CAPTURE_DURATION_S) -> Optional[np.ndarray]:
    """
    Capture audio using arecord in burst mode.
    Returns int16 numpy array or None on failure.

    Uses a short arecord invocation (not continuous) so the ALSA device
    is released between captures, avoiding conflicts with chronicle_ear.py
    which holds the device continuously. ALSA's plughw plugin handles
    sharing — both processes can open plughw:2,0 but not hw:2,0.
    """
    try:
        cmd = [
            "arecord",
            "-D", ALSA_DEVICE,
            "-f", "S16_LE",
            "-r", str(SAMPLE_RATE),
            "-c", str(CHANNELS),
            "-t", "raw",
            "-d", str(duration_s),
            "-q",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=duration_s + 5,
        )
        if result.returncode != 0:
            log.warning(f"arecord failed (rc={result.returncode})")
            return None

        raw = result.stdout
        expected = SAMPLE_RATE * duration_s * SAMPLE_WIDTH
        if len(raw) < expected * 0.8:
            log.warning(f"Short capture: {len(raw)} bytes (expected ~{expected})")
            return None

        return np.frombuffer(raw, dtype=np.int16)

    except subprocess.TimeoutExpired:
        log.warning("arecord timed out")
        return None
    except Exception as e:
        log.error(f"Capture error: {e}")
        return None


# ---------------------------------------------------------------------------
# Spectral feature extraction
# ---------------------------------------------------------------------------
def compute_features(audio: np.ndarray) -> Dict[str, float]:
    """
    Extract spectral features from raw int16 audio.
    Returns a dict of named features used for classification.
    """
    # Convert to float
    x = audio.astype(np.float32) / 32768.0

    # Overall energy
    rms = np.sqrt(np.mean(x ** 2))
    rms_int16 = rms * 32768.0

    # FFT over the full signal (or windowed segments)
    n_fft = 2048
    hop = 1024
    n_frames = max(1, (len(x) - n_fft) // hop)

    # Compute spectrogram (magnitude)
    specs = []
    for i in range(n_frames):
        frame = x[i * hop : i * hop + n_fft]
        if len(frame) < n_fft:
            break
        windowed = frame * np.hanning(n_fft)
        mag = np.abs(np.fft.rfft(windowed))
        specs.append(mag)

    if not specs:
        return {"rms": rms_int16, "silence": 1.0}

    spec = np.array(specs)  # (n_frames, n_freq_bins)
    freqs = np.fft.rfftfreq(n_fft, 1.0 / SAMPLE_RATE)

    # Mean spectrum across time
    mean_spec = np.mean(spec, axis=0)
    total_energy = np.sum(mean_spec) + 1e-10

    # --- Band energies ---
    def band_energy(lo, hi):
        mask = (freqs >= lo) & (freqs < hi)
        return np.sum(mean_spec[mask]) / total_energy

    sub_bass = band_energy(20, 100)       # rumble, HVAC
    bass = band_energy(100, 300)          # bass music, footsteps
    low_mid = band_energy(300, 1000)      # speech fundamentals
    mid = band_energy(1000, 3000)         # speech formants, music
    high_mid = band_energy(3000, 6000)    # sibilance, brightness
    high = band_energy(6000, 8000)        # air, cymbals

    speech_band = band_energy(300, 3000)  # core speech range

    # --- Spectral flatness (how noise-like vs tonal) ---
    # Geometric mean / arithmetic mean — 1.0 = white noise, 0.0 = pure tone
    log_spec = np.log(mean_spec + 1e-10)
    geo_mean = np.exp(np.mean(log_spec))
    arith_mean = np.mean(mean_spec)
    spectral_flatness = geo_mean / (arith_mean + 1e-10)

    # --- Spectral centroid (brightness) ---
    spectral_centroid = np.sum(freqs * mean_spec) / total_energy

    # --- Temporal variation (how much the spectrum changes frame to frame) ---
    if len(specs) > 1:
        diffs = np.diff(spec, axis=0)
        temporal_flux = np.mean(np.sqrt(np.mean(diffs ** 2, axis=1)))
    else:
        temporal_flux = 0.0

    # --- Onset detection (percussive events) ---
    # Count frames where energy jumps significantly
    frame_energies = np.sum(spec, axis=1)
    if len(frame_energies) > 1:
        energy_diff = np.diff(frame_energies)
        energy_std = np.std(frame_energies) + 1e-10
        onset_count = np.sum(energy_diff > 2.0 * energy_std)
        onset_rate = onset_count / (len(frame_energies) / (SAMPLE_RATE / hop))
    else:
        onset_rate = 0.0

    # --- Temporal energy variance (steady vs modulated) ---
    energy_variance = np.var(frame_energies) / (np.mean(frame_energies) ** 2 + 1e-10)

    # --- Periodicity / rhythm detection ---
    # Autocorrelation of frame energies to detect repeating patterns
    if len(frame_energies) > 20:
        fe_centered = frame_energies - np.mean(frame_energies)
        autocorr = np.correlate(fe_centered, fe_centered, mode="full")
        autocorr = autocorr[len(autocorr) // 2:]
        if autocorr[0] > 0:
            autocorr = autocorr / autocorr[0]
        # Look for peaks in the 0.3-3 second range (musical tempo range)
        lo_lag = int(0.3 * SAMPLE_RATE / hop)
        hi_lag = min(int(3.0 * SAMPLE_RATE / hop), len(autocorr) - 1)
        if hi_lag > lo_lag:
            rhythm_strength = np.max(autocorr[lo_lag:hi_lag])
        else:
            rhythm_strength = 0.0
    else:
        rhythm_strength = 0.0

    return {
        "rms": rms_int16,
        "sub_bass": sub_bass,
        "bass": bass,
        "low_mid": low_mid,
        "mid": mid,
        "high_mid": high_mid,
        "high": high,
        "speech_band": speech_band,
        "spectral_flatness": spectral_flatness,
        "spectral_centroid": spectral_centroid,
        "temporal_flux": temporal_flux,
        "onset_rate": onset_rate,
        "energy_variance": energy_variance,
        "rhythm_strength": rhythm_strength,
    }


# ---------------------------------------------------------------------------
# Scene classification from features
# ---------------------------------------------------------------------------
# Each classifier returns (label, confidence) where confidence is 0.0-1.0.
# We run all classifiers and return the top results.

def classify_scene(features: Dict[str, float]) -> List[Tuple[str, float]]:
    """
    Classify the audio scene based on spectral features.
    Returns a sorted list of (label, confidence) tuples, highest first.
    """
    scores: Dict[str, float] = {}

    rms = features["rms"]

    # --- Silence ---
    if rms < SILENCE_RMS_THRESHOLD:
        scores["silence"] = min(1.0, (SILENCE_RMS_THRESHOLD - rms) / SILENCE_RMS_THRESHOLD)
        # If it's truly silent, other categories are unlikely
        return sorted(scores.items(), key=lambda x: -x[1])

    # Normalize features for non-silent audio
    speech_band = features["speech_band"]
    flatness = features["spectral_flatness"]
    centroid = features["spectral_centroid"]
    flux = features["temporal_flux"]
    onset_rate = features["onset_rate"]
    energy_var = features["energy_variance"]
    rhythm = features["rhythm_strength"]
    sub_bass = features["sub_bass"]
    bass = features["bass"]

    # --- Speech ---
    # Strong speech band energy, moderate flatness, moderate temporal variation
    speech_score = 0.0
    if speech_band > 0.45:
        speech_score += 0.4
    elif speech_band > 0.35:
        speech_score += 0.25
    # Speech has moderate spectral flatness (not pure tone, not white noise)
    if 0.15 < flatness < 0.6:
        speech_score += 0.2
    # Speech modulates — not steady like a fan
    if 0.05 < energy_var < 2.0:
        speech_score += 0.2
    # Centroid in speech range
    if 500 < centroid < 2500:
        speech_score += 0.2
    scores["speech_nearby"] = min(1.0, speech_score)

    # --- Music ---
    # Broader frequency spread, rhythm, sustained energy
    music_score = 0.0
    # Music tends to have rhythm
    if rhythm > 0.3:
        music_score += 0.35
    elif rhythm > 0.15:
        music_score += 0.2
    # Music has energy across more bands
    band_spread = 1.0 - abs(speech_band - 0.5)  # Penalize if too concentrated
    if band_spread > 0.7:
        music_score += 0.15
    # Bass presence suggests music
    if bass > 0.15:
        music_score += 0.2
    # Moderate flatness (tonal content)
    if flatness < 0.4:
        music_score += 0.15
    # Sustained energy (low variance = steady)
    if energy_var < 0.5 and rms > 800:
        music_score += 0.15
    scores["music"] = min(1.0, music_score)

    # --- TV / Media ---
    # Compressed dynamic range (low variance), mix of speech and music features
    tv_score = 0.0
    if energy_var < 0.3 and rms > 600:
        tv_score += 0.3
    # If both speech and music score moderately, could be TV
    if scores.get("speech_nearby", 0) > 0.3 and scores.get("music", 0) > 0.2:
        tv_score += 0.3
    # Centroid tends mid-range for mixed content
    if 1000 < centroid < 3000:
        tv_score += 0.15
    # Not too flat (has tonal content) but not pure tone
    if 0.1 < flatness < 0.5:
        tv_score += 0.15
    scores["tv_media"] = min(1.0, tv_score)

    # --- Cooking ---
    # Percussive events (dishes), possibly some broadband noise (sizzling)
    cooking_score = 0.0
    if onset_rate > 1.5:
        cooking_score += 0.3
    # High frequency content (sizzling, clanking)
    if features["high_mid"] > 0.12:
        cooking_score += 0.2
    if features["high"] > 0.08:
        cooking_score += 0.15
    # High spectral flatness (sizzling is noise-like)
    if flatness > 0.5 and features["high_mid"] > 0.1:
        cooking_score += 0.2
    # Moderate overall energy
    if 500 < rms < 5000:
        cooking_score += 0.1
    scores["cooking"] = min(1.0, cooking_score)

    # --- Movement ---
    # Percussive + low frequency (footsteps, doors)
    movement_score = 0.0
    if onset_rate > 0.5 and onset_rate < 5.0:
        movement_score += 0.25
    # Low frequency emphasis (footsteps, door thuds)
    if sub_bass + bass > 0.35:
        movement_score += 0.25
    # Not too much high frequency (distinguishes from cooking)
    if features["high_mid"] < 0.1:
        movement_score += 0.15
    # Intermittent (high variance)
    if energy_var > 0.5:
        movement_score += 0.2
    scores["movement"] = min(1.0, movement_score)

    # --- Mechanical ---
    # Steady, narrow-band, low variance (fans, HVAC, fridge)
    mechanical_score = 0.0
    if energy_var < 0.1:
        mechanical_score += 0.35
    # Low spectral flatness = tonal hum
    if flatness < 0.2:
        mechanical_score += 0.25
    # Sub-bass or bass dominant (motor hum)
    if sub_bass + bass > 0.4:
        mechanical_score += 0.2
    # Low onset rate (steady)
    if onset_rate < 0.5:
        mechanical_score += 0.15
    scores["mechanical"] = min(1.0, mechanical_score)

    # --- Nature ---
    # Broadband noise (rain, wind), possibly with tonal elements (birds)
    nature_score = 0.0
    if flatness > 0.6:
        nature_score += 0.3
    # Broad spread — energy not concentrated in any one band
    max_band = max(sub_bass, bass, features["low_mid"], features["mid"],
                   features["high_mid"], features["high"])
    if max_band < 0.3:
        nature_score += 0.25
    # Low onset rate (rain is continuous, not percussive)
    if onset_rate < 1.0:
        nature_score += 0.15
    # Moderate temporal variation (wind gusts)
    if 0.1 < energy_var < 1.0:
        nature_score += 0.15
    scores["nature"] = min(1.0, nature_score)

    # --- Pets ---
    # Distinctive: energy in mid-to-high range, intermittent, tonal
    # Dog bark: sharp onset, 300-2000Hz
    # Cat meow: tonal, 700-1500Hz, sustained
    pets_score = 0.0
    if onset_rate > 1.0 and features["mid"] > 0.2:
        pets_score += 0.25
    if 0.1 < flatness < 0.4 and features["mid"] > 0.25:
        pets_score += 0.2
    if energy_var > 0.5:
        pets_score += 0.15
    scores["pets"] = min(1.0, pets_score)

    # Sort by confidence
    results = sorted(scores.items(), key=lambda x: -x[1])
    return results


# ---------------------------------------------------------------------------
# Result smoothing
# ---------------------------------------------------------------------------
class SceneHistory:
    """
    Keeps a rolling history of classifications to smooth out transient noise.
    A category must appear in multiple consecutive windows to be reported.
    """
    def __init__(self, maxlen: int = HISTORY_LEN):
        self.history: deque = deque(maxlen=maxlen)

    def add(self, results: List[Tuple[str, float]]):
        self.history.append(dict(results))

    def get_smoothed(self, top_n: int = 3) -> List[Tuple[str, float]]:
        """
        Return top-N categories averaged over history.
        Categories that appear more consistently get higher smoothed scores.
        """
        if not self.history:
            return [("silence", 1.0)]

        # Average scores across history, penalizing categories
        # that only appear in some windows
        all_labels = set()
        for h in self.history:
            all_labels.update(h.keys())

        averaged = {}
        n = len(self.history)
        for label in all_labels:
            scores = [h.get(label, 0.0) for h in self.history]
            # Weighted: recent windows matter more
            weights = np.linspace(0.5, 1.0, n)
            averaged[label] = np.average(scores, weights=weights)

        # Sort and return top N
        results = sorted(averaged.items(), key=lambda x: -x[1])
        return results[:top_n]


# ---------------------------------------------------------------------------
# MQTT publishing
# ---------------------------------------------------------------------------
def publish_mqtt(sounds: List[Tuple[str, float]]) -> bool:
    """
    Publish scene classification to MQTT.
    Uses paho-mqtt if available, falls back to mosquitto_pub CLI.
    """
    payload = {
        "sounds": [s[0] for s in sounds],
        "confidences": {s[0]: round(s[1], 3) for s in sounds},
        "top_confidence": round(sounds[0][1], 3) if sounds else 0.0,
        "timestamp": int(time.time()),
    }
    payload_str = json.dumps(payload)

    # Try paho-mqtt first
    try:
        import paho.mqtt.publish as mqtt_pub
        mqtt_pub.single(
            MQTT_TOPIC,
            payload=payload_str,
            hostname=MQTT_BROKER,
            port=MQTT_PORT,
            retain=True,
        )
        return True
    except ImportError:
        pass
    except Exception as e:
        log.warning(f"paho-mqtt publish failed: {e}")

    # Fallback: mosquitto_pub
    try:
        subprocess.run(
            ["mosquitto_pub", "-h", MQTT_BROKER, "-p", str(MQTT_PORT),
             "-t", MQTT_TOPIC, "-r", "-m", payload_str],
            capture_output=True, timeout=5,
        )
        return True
    except Exception as e:
        log.warning(f"mosquitto_pub failed: {e}")
        return False


def write_scene_file(sounds: List[Tuple[str, float]]):
    """
    Write latest scene to a JSON file for ambient_collector to read.
    Also writes a human-readable summary line.
    """
    # Human-readable descriptions for scene.txt integration
    HUMAN_LABELS = {
        "silence": "Quiet",
        "speech_nearby": "Conversation nearby",
        "music": "Music playing",
        "tv_media": "TV or media on",
        "cooking": "Kitchen sounds",
        "nature": "Nature sounds",
        "pets": "Pet sounds",
        "movement": "Movement sounds",
        "mechanical": "Mechanical hum",
    }

    labels = [s[0] for s in sounds if s[1] > 0.15]  # Only confident ones
    human = [HUMAN_LABELS.get(l, l) for l in labels]

    data = {
        "sounds": [s[0] for s in sounds],
        "confidences": {s[0]: round(s[1], 3) for s in sounds},
        "top_confidence": round(sounds[0][1], 3) if sounds else 0.0,
        "human_summary": ", ".join(human) if human else "Quiet",
        "timestamp": int(time.time()),
    }

    try:
        tmp = SCENE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, SCENE_FILE)
    except Exception as e:
        log.warning(f"Failed to write scene file: {e}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    log.info("Chronicle Ear Scene starting...")
    log.info(f"  Device: {ALSA_DEVICE}")
    log.info(f"  Capture: {CAPTURE_DURATION_S}s every {CYCLE_INTERVAL_S}s")
    log.info(f"  MQTT: {MQTT_BROKER}:{MQTT_PORT} -> {MQTT_TOPIC}")
    log.info(f"  Scene file: {SCENE_FILE}")

    history = SceneHistory()

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    while running:
        cycle_start = time.time()

        # 1. Capture audio
        audio = capture_audio()
        if audio is None:
            log.warning("Capture failed, will retry next cycle")
            _sleep_remaining(cycle_start, CYCLE_INTERVAL_S, running)
            continue

        # 2. Extract features
        features = compute_features(audio)

        # 3. Classify
        raw_results = classify_scene(features)

        # 4. Smooth with history
        history.add(raw_results)
        smoothed = history.get_smoothed(top_n=3)

        # 5. Log
        top_str = ", ".join(f"{s[0]}={s[1]:.2f}" for s in smoothed)
        log.info(f"Scene: {top_str}  (rms={features['rms']:.0f})")

        # 6. Publish
        publish_mqtt(smoothed)
        write_scene_file(smoothed)

        # 7. Sleep until next cycle
        _sleep_remaining(cycle_start, CYCLE_INTERVAL_S, running)

    log.info("Chronicle Ear Scene stopped.")


def _sleep_remaining(start: float, interval: float, running_flag: bool):
    """Sleep for the remainder of the cycle interval, checking running flag."""
    elapsed = time.time() - start
    remaining = max(0, interval - elapsed)
    # Sleep in small increments so we can respond to signals
    end = time.time() + remaining
    while time.time() < end and running_flag:
        time.sleep(min(1.0, end - time.time()))


if __name__ == "__main__":
    main()


# ===========================================================================
# YAMNET UPGRADE PATH
# ===========================================================================
#
# The spectral approach above works with zero dependencies beyond numpy.
# For much better accuracy (521 fine-grained sound classes), upgrade to YAMNet.
#
# Option A: YAMNet ONNX (preferred)
# -----------------------------------
# 1. Get the model:
#    - Check huggingface.co for "yamnet onnx" — there are community conversions
#    - Or convert from TFLite yourself:
#
#      pip install tensorflow tf2onnx
#      wget https://storage.googleapis.com/audioset/yamnet.tflite
#      python -m tf2onnx.convert --tflite yamnet.tflite --output yamnet.onnx
#
# 2. Get the class map:
#    wget https://raw.githubusercontent.com/tensorflow/models/master/research/audioset/yamnet/yamnet_class_map.csv
#
# 3. Replace classify_scene() with:
#
#    import onnxruntime as ort
#    import csv
#
#    class YAMNetClassifier:
#        BUCKET_MAP = {
#            "music": ["Music", "Musical instrument", "Singing", "Song", "Guitar",
#                       "Piano", "Drum", "Bass", "Synthesizer", "Orchestra",
#                       "Choir", "Hip hop music", "Rock music", "Pop music",
#                       "Electronic music", "Jazz", "Blues", "Country",
#                       "Classical music", "Reggae", "Folk music"],
#            "speech_nearby": ["Speech", "Conversation", "Narration", "Monologue",
#                              "Male speech", "Female speech", "Child speech",
#                              "Babbling", "Whispering", "Shout", "Laughter"],
#            "tv_media": ["Television", "Radio", "Jingle", "Theme music",
#                         "Background music", "Sound effect"],
#            "cooking": ["Sizzle", "Frying", "Boiling", "Microwave oven",
#                        "Blender", "Dishes", "Cutlery", "Chopping"],
#            "nature": ["Bird", "Bird vocalization", "Birdsong", "Wind",
#                       "Rain", "Thunder", "Water", "Stream", "Waves",
#                       "Rustling leaves", "Cricket", "Frog", "Insect"],
#            "pets": ["Dog", "Bark", "Howl", "Growl", "Whimper",
#                     "Cat", "Purr", "Meow", "Hiss"],
#            "movement": ["Footsteps", "Door", "Knock", "Keys jangling",
#                         "Drawer", "Cupboard", "Creak", "Squeak"],
#            "mechanical": ["Fan", "Air conditioning", "Refrigerator",
#                           "Washing machine", "Dryer", "Engine",
#                           "Mechanical fan", "HVAC", "Hum"],
#        }
#
#        def __init__(self, model_path, class_map_path):
#            self.session = ort.InferenceSession(model_path)
#            self.classes = self._load_class_map(class_map_path)
#            self.bucket_index = self._build_bucket_index()
#
#        def _load_class_map(self, path):
#            classes = {}
#            with open(path) as f:
#                reader = csv.DictReader(f)
#                for row in reader:
#                    classes[int(row["index"])] = row["display_name"]
#            return classes
#
#        def _build_bucket_index(self):
#            """Map each YAMNet class index to a bucket."""
#            index = {}
#            for bucket, keywords in self.BUCKET_MAP.items():
#                for idx, name in self.classes.items():
#                    for kw in keywords:
#                        if kw.lower() in name.lower():
#                            index[idx] = bucket
#                            break
#            return index
#
#        def classify(self, audio_float32):
#            """Run YAMNet on float32 audio, return bucketed results."""
#            # YAMNet expects (1, N) float32 audio at 16kHz
#            inp = audio_float32.reshape(1, -1)
#            scores = self.session.run(None, {"input": inp})[0]
#            # scores shape: (n_windows, 521) — average across windows
#            mean_scores = np.mean(scores, axis=0)
#
#            # Aggregate into buckets
#            bucket_scores = {}
#            for idx, score in enumerate(mean_scores):
#                bucket = self.bucket_index.get(idx)
#                if bucket:
#                    bucket_scores[bucket] = bucket_scores.get(bucket, 0) + score
#
#            # Normalize
#            max_score = max(bucket_scores.values()) if bucket_scores else 1.0
#            for k in bucket_scores:
#                bucket_scores[k] /= max_score
#
#            results = sorted(bucket_scores.items(), key=lambda x: -x[1])
#            return results
#
# 4. Model size: ~3.5 MB (ONNX), runs in ~200ms on Pi 5 CPU
#    Memory: ~50 MB extra during inference
#
# Option B: TFLite directly
# --------------------------
# If ONNX conversion is problematic, use tflite_runtime directly:
#    pip3 install tflite-runtime
#    Then load yamnet.tflite with tflite_runtime.Interpreter
#    Same class map and bucketing logic applies.
#
# ===========================================================================
