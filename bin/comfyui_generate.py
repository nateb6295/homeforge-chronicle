#!/usr/bin/env python3
"""ComfyUI image generation via RunPod serverless endpoint.

Usage:
  python3 comfyui_generate.py "a cosmic constellation of golden threads"
  python3 comfyui_generate.py "prompt" --output /path/to/output.png
  python3 comfyui_generate.py "prompt" --width 1024 --height 1024 --steps 25
  python3 comfyui_generate.py "prompt" --negative "blurry, low quality"
  python3 comfyui_generate.py --input image.png "style transfer prompt"
"""
import argparse
import base64
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

ENDPOINT_ID = "lb3ef0kobw9km6"
API_BASE = f"https://api.runpod.ai/v2/{ENDPOINT_ID}"


def get_api_key():
    key = os.environ.get("RUNPOD_API_KEY", "")
    if not key:
        env_file = Path.home() / "chronicle" / "chronicle.env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                cleaned = line.lstrip("export ")
                if cleaned.startswith("RUNPOD_API_KEY="):
                    key = cleaned.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    return key


def build_sdxl_workflow(prompt, negative="", width=1024, height=1024, steps=25,
                        cfg=7.0, seed=None, sampler="euler", scheduler="normal"):
    if seed is None:
        import random
        seed = random.randint(0, 2**32 - 1)

    return {
        "4": {
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            "class_type": "CheckpointLoaderSimple",
        },
        "6": {
            "inputs": {"text": prompt, "clip": ["4", 1]},
            "class_type": "CLIPTextEncode",
            "_meta": {"title": "Positive Prompt"},
        },
        "7": {
            "inputs": {"text": negative or "blurry, bad quality, distorted", "clip": ["4", 1]},
            "class_type": "CLIPTextEncode",
            "_meta": {"title": "Negative Prompt"},
        },
        "5": {
            "inputs": {"width": width, "height": height, "batch_size": 1},
            "class_type": "EmptyLatentImage",
        },
        "3": {
            "inputs": {
                "seed": seed,
                "steps": steps,
                "cfg": cfg,
                "sampler_name": sampler,
                "scheduler": scheduler,
                "denoise": 1.0,
                "model": ["4", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["5", 0],
            },
            "class_type": "KSampler",
        },
        "8": {
            "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
            "class_type": "VAEDecode",
        },
        "9": {
            "inputs": {"filename_prefix": "ComfyUI", "images": ["8", 0]},
            "class_type": "SaveImage",
        },
    }


def build_img2img_workflow(prompt, negative="", width=1024, height=1024, steps=20,
                           cfg=7.0, denoise=0.7, seed=None):
    if seed is None:
        import random
        seed = random.randint(0, 2**32 - 1)

    return {
        "4": {
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            "class_type": "CheckpointLoaderSimple",
        },
        "6": {
            "inputs": {"text": prompt, "clip": ["4", 1]},
            "class_type": "CLIPTextEncode",
            "_meta": {"title": "Positive Prompt"},
        },
        "7": {
            "inputs": {"text": negative or "blurry, bad quality, distorted", "clip": ["4", 1]},
            "class_type": "CLIPTextEncode",
            "_meta": {"title": "Negative Prompt"},
        },
        "10": {
            "inputs": {"image": "input_image.png", "upload": "image"},
            "class_type": "LoadImage",
        },
        "11": {
            "inputs": {"pixels": ["10", 0], "vae": ["4", 2]},
            "class_type": "VAEEncode",
        },
        "3": {
            "inputs": {
                "seed": seed,
                "steps": steps,
                "cfg": cfg,
                "sampler_name": "euler",
                "scheduler": "normal",
                "denoise": denoise,
                "model": ["4", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["11", 0],
            },
            "class_type": "KSampler",
        },
        "8": {
            "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
            "class_type": "VAEDecode",
        },
        "9": {
            "inputs": {"filename_prefix": "ComfyUI", "images": ["8", 0]},
            "class_type": "SaveImage",
        },
    }


def submit_job(workflow, api_key, images=None):
    payload = {"input": {"workflow": workflow}}
    if images:
        payload["input"]["images"] = images

    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{API_BASE}/run",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def poll_status(job_id, api_key, timeout=300, interval=5):
    url = f"{API_BASE}/status/{job_id}"
    start = time.time()
    while time.time() - start < timeout:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
        resp = urllib.request.urlopen(req, timeout=30)
        data = json.loads(resp.read())
        status = data.get("status", "UNKNOWN")

        if status == "COMPLETED":
            return data
        elif status == "FAILED":
            print(f"Job failed: {data.get('error', 'unknown error')}", file=sys.stderr)
            return data
        elif status == "CANCELLED":
            print("Job cancelled", file=sys.stderr)
            return data

        elapsed = int(time.time() - start)
        print(f"  [{elapsed}s] Status: {status}", file=sys.stderr)
        time.sleep(interval)

    print(f"Timeout after {timeout}s", file=sys.stderr)
    return None


def save_output(data, output_path):
    output = data.get("output", {})
    images = output.get("images", [])
    if not images:
        msg = output.get("message", "")
        if msg:
            print(f"Worker message: {msg}", file=sys.stderr)
        print("No images in output", file=sys.stderr)
        print(f"Full output keys: {list(output.keys())}", file=sys.stderr)
        return None

    img_data = images[0]
    if isinstance(img_data, dict) and "data" in img_data:
        raw = base64.b64decode(img_data["data"])
    elif isinstance(img_data, str):
        raw = base64.b64decode(img_data)
    else:
        print(f"Unknown image format: {type(img_data)}", file=sys.stderr)
        return None

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(raw)
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate images via ComfyUI on RunPod")
    parser.add_argument("prompt", help="Text prompt for generation")
    parser.add_argument("--output", "-o", help="Output file path",
                       default=str(Path.home() / "chronicle/data/visualizations/comfyui_output.png"))
    parser.add_argument("--negative", "-n", default="", help="Negative prompt")
    parser.add_argument("--width", type=int, default=1024)
    parser.add_argument("--height", type=int, default=1024)
    parser.add_argument("--steps", type=int, default=25)
    parser.add_argument("--cfg", type=float, default=7.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--input", "-i", dest="input_image", help="Input image for img2img")
    parser.add_argument("--denoise", type=float, default=0.7, help="Denoise strength for img2img")
    parser.add_argument("--timeout", type=int, default=300, help="Max wait time in seconds")
    args = parser.parse_args()

    api_key = get_api_key()
    if not api_key:
        print("Error: RUNPOD_API_KEY not found", file=sys.stderr)
        sys.exit(1)

    images = None
    if args.input_image:
        input_path = Path(args.input_image)
        if not input_path.exists():
            print(f"Input image not found: {input_path}", file=sys.stderr)
            sys.exit(1)
        with open(input_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        images = [{"name": "input_image.png", "image": f"data:image/png;base64,{img_b64}"}]
        workflow = build_img2img_workflow(
            args.prompt, args.negative, args.width, args.height,
            args.steps, args.cfg, args.denoise, args.seed,
        )
        print(f"Mode: img2img (denoise={args.denoise})")
    else:
        workflow = build_sdxl_workflow(
            args.prompt, args.negative, args.width, args.height,
            args.steps, args.cfg, args.seed,
        )
        print(f"Mode: text-to-image")

    print(f"Prompt: {args.prompt[:80]}...")
    print(f"Size: {args.width}x{args.height}, Steps: {args.steps}, CFG: {args.cfg}")
    print(f"Submitting to ComfyUI SDXL endpoint...")

    result = submit_job(workflow, api_key, images)
    job_id = result.get("id")
    if not job_id:
        print(f"Failed to submit: {result}", file=sys.stderr)
        sys.exit(1)

    print(f"Job ID: {job_id}")
    print(f"Polling (timeout={args.timeout}s)...")

    data = poll_status(job_id, api_key, timeout=args.timeout)
    if not data or data.get("status") != "COMPLETED":
        print("Generation failed", file=sys.stderr)
        if data:
            print(json.dumps(data, indent=2)[:500], file=sys.stderr)
        sys.exit(1)

    delay = data.get("delayTime", 0)
    exec_time = data.get("executionTime", 0)
    print(f"Completed: queue={delay}ms, execution={exec_time}ms")

    saved = save_output(data, args.output)
    if saved:
        size = os.path.getsize(saved)
        print(f"Saved: {saved} ({size:,} bytes)")
    else:
        print("Could not extract image from output", file=sys.stderr)
        print(json.dumps(data.get("output", {}), indent=2)[:500], file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
