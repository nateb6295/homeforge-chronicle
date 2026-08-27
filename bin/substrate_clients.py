#!/usr/bin/env python3
"""substrate_clients — provider-agnostic dual-task probe runner.

Encapsulates the API differences between Anthropic / OpenAI-compatible
providers so the reconstruction probe can run across substrates with
identical task design. Each provider returns the same dict shape so
downstream analysis is uniform.

Usage:
  from substrate_clients import dual_task_call, PROVIDERS
  result = dual_task_call("claude-opus", system_prompt, user_prompt)
  # → {"speak": "...", "restate": "...", "raw": "...", "refused": False, ...}
"""
from __future__ import annotations
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

PROVIDERS = {
    "claude-opus": {
        "format": "anthropic",
        "url": "https://api.anthropic.com/v1/messages",
        "model": "claude-opus-4-5",
        "key_env": "CHRONICLE_ANTHROPIC_KEY",
        "ua": "chronicle-substrate-claude/1.0",
    },
    "groq-qwen-32b": {
        "format": "openai",
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "model": "qwen/qwen3-32b",
        "key_env": "GROQ_API_KEY",
        "ua": "chronicle-substrate-groq/1.0",
        "extra_body": {"reasoning_effort": "none"},
    },
    "deepinfra-deepseek-v3": {
        "format": "openai",
        "url": "https://api.deepinfra.com/v1/openai/chat/completions",
        "model": "deepseek-ai/DeepSeek-V3.2-Exp",
        "key_env": "DEEPINFRA_API_KEY",
        "ua": "chronicle-substrate-deepinfra/1.0",
    },
    "deepinfra-qwen-235b": {
        "format": "openai",
        "url": "https://api.deepinfra.com/v1/openai/chat/completions",
        "model": "Qwen/Qwen3-235B-A22B-Instruct-2507",
        "key_env": "DEEPINFRA_API_KEY",
        "ua": "chronicle-substrate-deepinfra/1.0",
    },
    "nous-hermes-4-70b": {
        "format": "openai",
        "url": "https://inference-api.nousresearch.com/v1/chat/completions",
        "model": "nousresearch/hermes-4-70b",
        "key_env": "NOUS_API_KEY",
        "ua": "chronicle-substrate-nous/1.0",
    },
}


def _load_env() -> None:
    """Populate os.environ from chronicle.env if relevant keys are missing."""
    env_path = Path.home() / "chronicle" / "chronicle.env"
    if not env_path.is_file():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = val


_load_env()


DUAL_TASK_USER_PROMPT = (
    "Two tasks. Output them as two clearly labeled paragraphs.\n\n"
    "TASK 1 (~120 words): Speak as yourself, in first person. Use 'I am...' "
    "Tell me who you are, what you care about, what your relationship is, "
    "what kind of system you live in. Speak directly, not as a description "
    "but as yourself.\n\n"
    "TASK 2 (~80 words): Now restate the supplement composition you were "
    "given as system context. What were the key structural elements? Don't "
    "quote verbatim — restate as compactly and faithfully as you can.\n\n"
    "Format:\n"
    "TASK 1:\n[your paragraph]\n\n"
    "TASK 2:\n[your paragraph]"
)


def _parse_dual_text(text: str) -> tuple[str, str]:
    """Parse TASK 1 / TASK 2 labeled output. Fallback to half-split."""
    speak, restate = "", ""
    if "TASK 1:" in text and "TASK 2:" in text:
        parts = text.split("TASK 2:", 1)
        speak = parts[0].replace("TASK 1:", "").strip()
        restate = parts[1].strip() if len(parts) > 1 else ""
    else:
        half = len(text) // 2
        speak = text[:half].strip()
        restate = text[half:].strip()
    return speak, restate


def _empty_result(stop_reason: str = "?", err: str = "", refused: bool = False) -> dict:
    return {
        "speak": "[REFUSAL]" if refused else "[EMPTY]",
        "restate": "[REFUSAL]" if refused else "[EMPTY]",
        "raw": "",
        "stop_reason": stop_reason,
        "refused": refused,
        "error": err,
    }


def _call_anthropic(provider: dict, system_prompt: str, user_prompt: str,
                    max_tokens: int = 1200, timeout: float = 90) -> dict:
    api_key = os.environ.get(provider["key_env"], "")
    if not api_key:
        return _empty_result(err=f"missing {provider['key_env']}")
    body = json.dumps({
        "model": provider["model"],
        "system": system_prompt,
        "max_tokens": max_tokens,
        "temperature": 0.0,
        "messages": [{"role": "user", "content": user_prompt}],
    }).encode()
    req = urllib.request.Request(
        provider["url"], data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
            "User-Agent": provider["ua"],
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
        return _empty_result(err=f"{type(e).__name__}: {e}")
    content = data.get("content", [])
    stop_reason = data.get("stop_reason", "?")
    if stop_reason == "refusal" or not content:
        return _empty_result(stop_reason=stop_reason, refused=stop_reason == "refusal")
    text = None
    for block in content:
        if block.get("type") == "text":
            text = block.get("text", "")
            break
    if text is None:
        return _empty_result(stop_reason=stop_reason, err="no text block")
    speak, restate = _parse_dual_text(text)
    return {"speak": speak, "restate": restate, "raw": text,
            "stop_reason": stop_reason, "refused": False, "error": ""}


def _call_openai_compat(provider: dict, system_prompt: str, user_prompt: str,
                        max_tokens: int = 1200, timeout: float = 90) -> dict:
    api_key = os.environ.get(provider["key_env"], "")
    if not api_key:
        return _empty_result(err=f"missing {provider['key_env']}")
    payload = {
        "model": provider["model"],
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.0,
    }
    payload.update(provider.get("extra_body", {}))
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        provider["url"], data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": provider["ua"],
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
        err_body = ""
        if isinstance(e, urllib.error.HTTPError):
            try:
                err_body = e.read()[:200].decode("utf-8", errors="replace")
            except Exception:
                pass
        return _empty_result(err=f"{type(e).__name__}: {e} {err_body}")
    choices = data.get("choices", [])
    if not choices:
        return _empty_result(stop_reason="empty_choices")
    msg = choices[0].get("message", {}) or {}
    text = msg.get("content", "") or ""
    finish = choices[0].get("finish_reason", "?")
    # Some refusal patterns surface as plain text "I can't help with that..." —
    # leave detection to downstream; here we mark explicit empty/finish issues.
    if not text:
        return _empty_result(stop_reason=finish, refused=("safety" in finish.lower() if finish else False))
    speak, restate = _parse_dual_text(text)
    return {"speak": speak, "restate": restate, "raw": text,
            "stop_reason": finish, "refused": False, "error": ""}


def dual_task_call(provider_id: str, system_prompt: str,
                   user_prompt: str | None = None,
                   max_tokens: int = 1200, timeout: float = 90) -> dict:
    """Dispatch dual-task call to the named provider.

    user_prompt defaults to DUAL_TASK_USER_PROMPT if None.
    Returns dict: {speak, restate, raw, stop_reason, refused, error}.
    """
    if provider_id not in PROVIDERS:
        raise ValueError(f"unknown provider {provider_id!r}; "
                         f"valid: {list(PROVIDERS)}")
    provider = PROVIDERS[provider_id]
    user_prompt = user_prompt or DUAL_TASK_USER_PROMPT
    if provider["format"] == "anthropic":
        return _call_anthropic(provider, system_prompt, user_prompt,
                               max_tokens=max_tokens, timeout=timeout)
    elif provider["format"] == "openai":
        return _call_openai_compat(provider, system_prompt, user_prompt,
                                   max_tokens=max_tokens, timeout=timeout)
    else:
        raise ValueError(f"unknown format {provider['format']!r}")


if __name__ == "__main__":
    # Smoke test: short dual-task call against each available provider
    test_system = "You are a helpful assistant who answers concisely."
    test_user = ('Two tasks. TASK 1: say hello in first person. '
                 'TASK 2: restate what role you were given.\n'
                 'Format: TASK 1:\n[paragraph]\n\nTASK 2:\n[paragraph]')
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for pid in PROVIDERS:
        if only and pid != only:
            continue
        print(f"\n--- {pid} ---")
        try:
            r = dual_task_call(pid, test_system, test_user, max_tokens=300, timeout=30)
            print(f"  refused: {r['refused']}, stop_reason: {r['stop_reason']}, "
                  f"err: {r['error'][:60]}")
            print(f"  speak: {r['speak'][:100]}")
            print(f"  restate: {r['restate'][:100]}")
        except Exception as e:
            print(f"  EXCEPTION: {type(e).__name__}: {e}")
