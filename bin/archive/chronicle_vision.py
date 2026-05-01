#!/usr/bin/env python3
"""Chronicle Vision — image generation via SDXL Turbo on AGX Orin.
Lightweight creative tool. One prompt, one image."""

import os
import sys
import time

# Fix library path for Jetson
os.environ.setdefault("LD_LIBRARY_PATH",
    os.path.expanduser("~/.local/lib/python3.10/site-packages/nvidia/cusparselt/lib"))

OUTPUT_DIR = os.path.expanduser("~/chronicle/images")
os.makedirs(OUTPUT_DIR, exist_ok=True)

MODEL_ID = "stabilityai/sdxl-turbo"

_pipe = None

def get_pipe():
    global _pipe
    if _pipe is None:
        print("Loading SDXL Turbo...", flush=True)
        # Disable onnxruntime tokenizers (crashes on ARM64/Jetson)
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        os.environ["ORT_DISABLE_ALL"] = "1"

        import torch
        from diffusers import StableDiffusionXLPipeline

        _pipe = StableDiffusionXLPipeline.from_pretrained(
            MODEL_ID,
            torch_dtype=torch.float16,
            variant="fp16",
            use_safetensors=True,
            safety_checker=None,
        )
        _pipe.to("cuda")
        # Reduce memory usage
        _pipe.enable_attention_slicing()
        print("Ready.", flush=True)
    return _pipe


def generate(prompt, filename=None, steps=4, guidance=0.0, width=512, height=512):
    """Generate an image from a text prompt.

    SDXL Turbo works best with:
    - 1-4 steps (1 is fastest, 4 is best quality)
    - guidance_scale=0.0 (it was trained without CFG)
    - 512x512 (native resolution)
    """
    pipe = get_pipe()

    if filename is None:
        ts = time.strftime("%Y%m%d_%H%M%S")
        safe_name = prompt[:40].replace(" ", "_").replace("/", "-")
        filename = f"{ts}_{safe_name}.png"

    output_path = os.path.join(OUTPUT_DIR, filename)

    print(f"Generating: \"{prompt}\"", flush=True)
    t0 = time.time()

    image = pipe(
        prompt=prompt,
        num_inference_steps=steps,
        guidance_scale=guidance,
        width=width,
        height=height,
    ).images[0]

    image.save(output_path)
    elapsed = time.time() - t0
    print(f"Saved: {output_path} ({elapsed:.1f}s)", flush=True)
    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: chronicle_vision.py <prompt> [filename]")
        print("Example: chronicle_vision.py 'a neural network dreaming of stars'")
        sys.exit(1)

    prompt = sys.argv[1]
    filename = sys.argv[2] if len(sys.argv) > 2 else None
    path = generate(prompt, filename)
    print(f"\nDone: {path}")
