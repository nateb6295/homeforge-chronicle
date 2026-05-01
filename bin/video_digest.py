#!/usr/bin/env python3
"""Video Digest — Extract transcript from YouTube videos.

Usage:
  python3 video_digest.py <url>              # Print transcript
  python3 video_digest.py <url> --summarize  # Print transcript + Gemma summary

Strategy:
  1. Try YouTube captions API first (fast, no download)
  2. Fall back to yt-dlp audio + faster-whisper (local, GPU-aware)

Note: faster-whisper uses CPU by default to avoid conflicts with Gemma on GPU.
Set VIDEO_WHISPER_DEVICE=cuda to use GPU (ensure Gemma is unloaded first).
"""
import json
import os
import re
import subprocess
import sys
import tempfile

GEMMA_URL = os.environ.get("GEMMA_URL", "http://localhost:11435")
GEMMA_MODEL = os.environ.get("GEMMA_MODEL", "gemma-4-26B-A4B-it-Q4_K_M.gguf")
WHISPER_MODEL = os.environ.get("VIDEO_WHISPER_MODEL", "base")
WHISPER_DEVICE = os.environ.get("VIDEO_WHISPER_DEVICE", "cpu")


def extract_video_id(url):
    """Extract YouTube video ID from various URL formats."""
    patterns = [
        r'(?:v=|/v/|youtu\.be/)([a-zA-Z0-9_-]{11})',
        r'(?:embed/)([a-zA-Z0-9_-]{11})',
        r'(?:shorts/)([a-zA-Z0-9_-]{11})',
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def try_captions(video_id):
    """Try to get transcript via YouTube captions API (no download needed)."""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        ytt_api = YouTubeTranscriptApi()
        transcript = ytt_api.fetch(video_id)
        lines = []
        for entry in transcript:
            lines.append(entry.text)
        return " ".join(lines)
    except Exception as e:
        print(f"Captions unavailable: {e}", file=sys.stderr)
        return None


def try_whisper(url):
    """Download audio via yt-dlp and transcribe with faster-whisper."""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            audio_path = os.path.join(tmpdir, "audio.wav")

            # Download audio only
            print("Downloading audio...", file=sys.stderr)
            result = subprocess.run(
                ["yt-dlp", "-x", "--audio-format", "wav",
                 "--audio-quality", "5",  # lower quality = smaller file = faster
                 "-o", audio_path, url],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode != 0:
                print(f"yt-dlp error: {result.stderr[:200]}", file=sys.stderr)
                return None

            # Find the actual output file (yt-dlp may add extension)
            import glob
            wav_files = glob.glob(os.path.join(tmpdir, "audio*"))
            if not wav_files:
                print("No audio file produced", file=sys.stderr)
                return None
            audio_file = wav_files[0]

            # Transcribe with faster-whisper
            print(f"Transcribing with faster-whisper ({WHISPER_MODEL}, {WHISPER_DEVICE})...", file=sys.stderr)
            from faster_whisper import WhisperModel
            model = WhisperModel(WHISPER_MODEL, device=WHISPER_DEVICE, compute_type="int8")
            segments, info = model.transcribe(audio_file, beam_size=5)

            text_parts = []
            for segment in segments:
                text_parts.append(segment.text.strip())

            return " ".join(text_parts)

    except Exception as e:
        print(f"Whisper error: {e}", file=sys.stderr)
        return None


def summarize(text, url):
    """Summarize transcript via Gemma."""
    import requests
    prompt = f"""Summarize this video transcript. Extract:
1. The main argument or thesis (1-2 sentences)
2. Key claims or findings (bullet points)
3. The most surprising or non-obvious point

Keep under 500 words. Be specific — names, numbers, concrete claims.

Transcript:
{text[:6000]}"""

    try:
        resp = requests.post(
            f"{GEMMA_URL}/v1/chat/completions",
            json={
                "model": GEMMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 600,
                "temperature": 0.3,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"Gemma summary error: {e}", file=sys.stderr)
        return None


def main():
    if len(sys.argv) < 2:
        print("Usage: video_digest.py <youtube-url> [--summarize]")
        sys.exit(1)

    url = sys.argv[1]
    do_summary = "--summarize" in sys.argv or "--summary" in sys.argv

    video_id = extract_video_id(url)
    if not video_id:
        print(f"Could not extract video ID from: {url}", file=sys.stderr)
        sys.exit(1)

    print(f"Video ID: {video_id}", file=sys.stderr)

    # Try captions first (fast)
    transcript = try_captions(video_id)

    # Fall back to whisper
    if not transcript:
        transcript = try_whisper(url)

    if not transcript:
        print("Failed to extract transcript from video.", file=sys.stderr)
        sys.exit(1)

    word_count = len(transcript.split())
    print(f"Transcript: {word_count} words", file=sys.stderr)

    if do_summary:
        summary = summarize(transcript, url)
        if summary:
            print(f"=== SUMMARY ===\n{summary}\n")
            print(f"=== TRANSCRIPT ({word_count} words) ===")

    print(transcript)


if __name__ == "__main__":
    main()
