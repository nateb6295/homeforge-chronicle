#!/usr/bin/env python3
"""hear.py — Opus's ears. Transcribe audio from any video URL.

Usage:
  hear.py URL                     # transcribe video/audio at URL
  hear.py URL --summary           # transcribe + print condensed summary
  hear.py FILE                    # transcribe a local audio/video file
  hear.py URL --save              # also save transcript to ~/chronicle/data/transcripts/

Supports: YouTube, X/Twitter, Reddit, TikTok, Instagram, Vimeo,
and hundreds more via yt-dlp. Also handles direct .mp4/.mp3 URLs.

Downloads audio only (no video), runs faster-whisper on GPU, prints text.
"""
import os
import re
import subprocess
import sys
import tempfile
import time

TRANSCRIPT_DIR = os.path.expanduser("~/chronicle/data/transcripts")
WHISPER_MODEL = "base"  # small footprint, fast on GPU. upgrade to "small" or "medium" if needed

X_PATTERN = re.compile(r'https?://(?:x\.com|twitter\.com)/\w+/status/(\d+)')


def load_env():
    for envfile in ["~/chronicle/chronicle.env", "~/.hermes/.env"]:
        path = os.path.expanduser(envfile)
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())


def resolve_x_video(tweet_id):
    """Use X API to get direct video URL from a tweet."""
    import urllib.request
    import json
    token = os.environ.get("X_BEARER_TOKEN", "")
    if not token:
        return None
    url = (
        f"https://api.x.com/2/tweets/{tweet_id}"
        f"?expansions=attachments.media_keys"
        f"&media.fields=url,type,variants"
    )
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "User-Agent": "Chronicle/1.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"X API error: {e}", file=sys.stderr)
        return None

    best_url = None
    best_bitrate = 0
    for m in data.get("includes", {}).get("media", []):
        if m.get("type") != "video":
            continue
        for v in m.get("variants", []):
            if v.get("content_type") == "video/mp4":
                br = v.get("bit_rate", 0)
                if br >= best_bitrate:
                    best_bitrate = br
                    best_url = v["url"]
    return best_url


def download_audio(url, dest_dir):
    """Download audio-only from URL via yt-dlp. Returns path to audio file."""
    out_template = os.path.join(dest_dir, "audio.%(ext)s")
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "-x",                          # extract audio only
        "--audio-format", "wav",       # wav for whisper compatibility
        "--audio-quality", "0",
        "-o", out_template,
        "--no-warnings",
        "--quiet",
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "Unsupported URL" in stderr or "not a valid URL" in stderr:
            return None
        if stderr:
            print(f"yt-dlp warning: {stderr[:200]}", file=sys.stderr)

    for f in os.listdir(dest_dir):
        if f.startswith("audio"):
            return os.path.join(dest_dir, f)
    return None


def download_direct(url, dest_dir):
    """Download a direct media URL via curl."""
    ext = "mp4"
    for e in ("mp3", "wav", "m4a", "ogg", "mp4", "webm"):
        if f".{e}" in url.lower():
            ext = e
            break
    dest = os.path.join(dest_dir, f"audio.{ext}")
    result = subprocess.run(
        ["curl", "-sL", "-o", dest, url],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode == 0 and os.path.getsize(dest) > 1000:
        return dest
    return None


def convert_to_wav(path, dest_dir):
    """Convert any audio/video to wav for whisper."""
    dest = os.path.join(dest_dir, "converted.wav")
    subprocess.run(
        ["ffmpeg", "-i", path, "-ar", "16000", "-ac", "1", "-y", dest],
        capture_output=True, timeout=120,
    )
    if os.path.exists(dest) and os.path.getsize(dest) > 1000:
        return dest
    return None


def transcribe(audio_path):
    """Run faster-whisper on audio file. Returns list of (start, end, text) segments."""
    from faster_whisper import WhisperModel
    try:
        model = WhisperModel(WHISPER_MODEL, device="cuda", compute_type="float16")
    except ValueError:
        model = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")
    segments, info = model.transcribe(audio_path, beam_size=5)
    results = []
    for seg in segments:
        results.append((seg.start, seg.end, seg.text.strip()))
    return results, info


def format_transcript(segments, include_timestamps=True):
    """Format segments into readable text."""
    lines = []
    for start, end, text in segments:
        if not text:
            continue
        if include_timestamps:
            m, s = divmod(int(start), 60)
            lines.append(f"[{m:02d}:{s:02d}] {text}")
        else:
            lines.append(text)
    return "\n".join(lines)


def main():
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return

    load_env()

    save = "--save" in args
    summary = "--summary" in args
    args = [a for a in args if a not in ("--save", "--summary")]
    target = args[0]

    # Resolve X/Twitter video URLs via API (yt-dlp can't extract X videos anymore)
    x_match = X_PATTERN.match(target)
    if x_match:
        tweet_id = x_match.group(1)
        print(f"X tweet detected. Resolving video via API...", file=sys.stderr)
        video_url = resolve_x_video(tweet_id)
        if video_url:
            print(f"Found video: {video_url[:80]}...", file=sys.stderr)
            target = video_url
        else:
            print("No video found in this tweet.", file=sys.stderr)
            sys.exit(1)

    is_local = os.path.isfile(target)

    with tempfile.TemporaryDirectory(prefix="hear_") as tmpdir:
        audio_path = None

        if is_local:
            audio_path = convert_to_wav(target, tmpdir)
            if not audio_path:
                audio_path = target
            source_label = os.path.basename(target)
        else:
            print(f"Downloading audio from: {target[:80]}...", file=sys.stderr)
            audio_path = download_audio(target, tmpdir)

            if not audio_path:
                print("yt-dlp failed, trying direct download...", file=sys.stderr)
                raw = download_direct(target, tmpdir)
                if raw:
                    audio_path = convert_to_wav(raw, tmpdir) or raw

            if not audio_path:
                print("ERROR: Could not download audio from URL.", file=sys.stderr)
                sys.exit(1)
            source_label = target[:80]

        size_mb = os.path.getsize(audio_path) / (1024 * 1024)
        print(f"Audio: {size_mb:.1f} MB. Transcribing...", file=sys.stderr)

        t0 = time.time()
        segments, info = transcribe(audio_path)
        elapsed = time.time() - t0

        duration_min = info.duration / 60 if info.duration else 0
        print(
            f"Done. {len(segments)} segments, {duration_min:.1f} min audio, "
            f"{elapsed:.1f}s to transcribe. Language: {info.language} ({info.language_probability:.0%})",
            file=sys.stderr,
        )

        transcript = format_transcript(segments, include_timestamps=True)
        plain = format_transcript(segments, include_timestamps=False)

        print(transcript)

        if save:
            os.makedirs(TRANSCRIPT_DIR, exist_ok=True)
            slug = re.sub(r'[^\w]', '_', source_label)[:60]
            ts = int(time.time())
            out_path = os.path.join(TRANSCRIPT_DIR, f"{ts}_{slug}.txt")
            with open(out_path, "w") as f:
                f.write(f"Source: {target}\n")
                f.write(f"Duration: {duration_min:.1f} min\n")
                f.write(f"Language: {info.language}\n")
                f.write(f"Transcribed: {time.strftime('%Y-%m-%d %H:%M')}\n")
                f.write("---\n\n")
                f.write(transcript)
            print(f"\nSaved to: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
