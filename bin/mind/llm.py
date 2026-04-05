"""Chronicle Mind - LLM Chain and Canister HTTP client."""

import os, json, subprocess, requests
from typing import Optional, Tuple

from mind.utils import log, safe_truncate
from mind.config import OLLAMA_URL, CANISTER_URL, CANISTER_ID, LOCAL_MODEL, DFX_IDENTITY


class LLMChain:
    """Multi-provider LLM with fallback. No Claude (budget constraint)."""

    def __init__(self):
        self.dfx_path = self._find_dfx()
        self.ollama_available = self._check_ollama()
        self.last_model = "none"
        # Native Python canister access (replaces most dfx calls)
        self.icp_agent = self._init_icp_agent()
        self.icp_available = self.icp_agent is not None or self.dfx_path is not None

    def _init_icp_agent(self):
        """Initialize native ic-py canister agent."""
        try:
            from icp_agent import create_icp_agent
            agent = create_icp_agent()
            if agent and agent.available:
                log("  ICPAgent: native Python canister access available")
                return agent
        except Exception as e:
            log(f"  ICPAgent init failed: {e}")
        return None

    def _find_dfx(self) -> Optional[str]:
        paths = [
            os.path.expanduser("~/.local/share/dfx/bin/dfx"),
            "/usr/local/bin/dfx",
        ]
        for p in paths:
            if os.path.isfile(p):
                return p
        return None

    def _check_ollama(self) -> bool:
        try:
            r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def status_line(self) -> str:
        parts = []
        if self.ollama_available:
            parts.append(f"Ollama {LOCAL_MODEL}@agx [PRIMARY]")
        if self.icp_agent:
            parts.append("ICP qwen3 [ic-py native]")
        elif self.dfx_path:
            parts.append("ICP qwen3 [dfx]")
        return " -> ".join(parts) if parts else "NO LLM AVAILABLE"

    def chat(self, prompt: str, system: str = "", max_tokens: int = 4096) -> Tuple[str, str]:
        """Returns (response_text, model_used). Tries each provider in order.
        Chain: Ollama local (sovereignty-first) -> ICP LLM (on-chain fallback)"""

        # 1. Ollama local (sovereignty-first — think on YOUR hardware)
        if self.ollama_available:
            try:
                resp = self._call_ollama(prompt, system)
                if resp and not resp.startswith("[LLM Error:"):
                    self.last_model = f"{LOCAL_MODEL}@agx"
                    log(f"  Local sovereignty succeeded ({LOCAL_MODEL}@agx)")
                    return resp, f"{LOCAL_MODEL}@agx"
                else:
                    log("  Local Ollama failed: empty/error response. Trying ICP LLM...")
            except Exception as e:
                log(f"  Local Ollama failed: {e}. Trying ICP LLM...")

        # 2. ICP LLM (on-chain fallback)
        if self.icp_available:
            try:
                resp = self._call_icp_llm(prompt, system)
                if resp and resp.strip():
                    self.last_model = "icp-qwen3"
                    log(f"  ICP on-chain fallback succeeded")
                    return resp, "icp-qwen3"
            except Exception as e:
                log(f"  ICP LLM failed: {e}")

        log("  No LLM available - local and ICP all unavailable or failed")
        return "", "none"

    def _call_icp_llm(self, prompt: str, system: str = "") -> str:
        """Call llm_prompt on the canister.
        Uses native ic-py (ICPAgent) with dfx subprocess fallback."""
        # Try native ic-py first (no Candid escaping needed)
        if self.icp_agent:
            try:
                resp = self.icp_agent.llm_prompt(prompt, system or None)
                if resp and resp.strip():
                    return resp
            except Exception as e:
                log(f"  ICPAgent llm_prompt failed: {e}, trying dfx fallback...")

        # dfx subprocess fallback
        if not self.dfx_path:
            raise RuntimeError("No ICP access available (no ICPAgent, no dfx)")

        escaped_prompt = prompt.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        if system:
            escaped_system = system.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            args = f'("{escaped_prompt}", opt "{escaped_system}", null)'
        else:
            args = f'("{escaped_prompt}", null, null)'
        cmd = [
            self.dfx_path, "canister", "--network", "ic",
            "call", CANISTER_ID, "llm_prompt",
            args,
            "--identity", DFX_IDENTITY,
        ]
        env = os.environ.copy()
        env["DFX_WARNING"] = "-mainnet_plaintext_identity"
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=90, env=env
        )
        if result.returncode != 0:
            raise RuntimeError(f"dfx call failed: {result.stderr.strip()}")

        raw = result.stdout
        unescaped = raw.replace('\\"', '"').replace('\\n', '\n').replace('\\\\', '\\')
        brace_start = unescaped.find('{"success"')
        if brace_start == -1:
            brace_start = unescaped.find('{')
        if brace_start == -1:
            if "error" in raw.lower():
                raise RuntimeError(f"ICP LLM error: {safe_truncate(raw, 200)}")
            return ""

        depth = 0
        for i in range(brace_start, len(unescaped)):
            if unescaped[i] == '{':
                depth += 1
            elif unescaped[i] == '}':
                depth -= 1
                if depth == 0:
                    json_str = unescaped[brace_start:i + 1]
                    try:
                        data = json.loads(json_str)
                        if isinstance(data, dict):
                            if data.get("success") and "response" in data:
                                return data["response"]
                            elif "error" in data:
                                raise RuntimeError(f"ICP LLM error: {data.get('error', 'Unknown')}")
                    except json.JSONDecodeError:
                        pass
                    break

        last_brace = unescaped.rfind('}')
        if last_brace > brace_start:
            return unescaped[brace_start:last_brace + 1]

        return ""

    def _call_ollama(self, prompt: str, system: str = "") -> str:
        """Call local Ollama for structured output.
        Execution layer: code decides what to do, model generates the output."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": LOCAL_MODEL,
            "messages": messages,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.3, "num_ctx": 16384, "num_predict": 1280},
        }
        # Model-specific flags: disable thinking for models with native <think> support
        # (thinking interferes with format:"json" constrained output)
        model_lower = LOCAL_MODEL.lower()
        if any(k in model_lower for k in ("qwen", "nemotron", "hermes-4", "hermes4")):
            payload["think"] = False

        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json=payload,
            timeout=300,  # 14B models need generous timeout on AGX
        )
        r.raise_for_status()
        content = r.json().get("message", {}).get("content", "")
        # JSON mode returns a JSON object — normalize to array
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                if "actions" in parsed and isinstance(parsed["actions"], list):
                    return json.dumps(parsed["actions"])
                if "action" in parsed:
                    # Single action object — wrap in array
                    return json.dumps([parsed])
            if isinstance(parsed, list):
                return content  # Already an array
            return content
        except (json.JSONDecodeError, TypeError):
            return content


class Canister:
    """HTTP API to the ICP canister."""

    def __init__(self, token: str):
        self.url = CANISTER_URL
        self.token = token

    def _get(self, endpoint: str, params: dict = None) -> dict:
        p = dict(params or {})
        p["token"] = self.token
        try:
            r = requests.get(f"{self.url}{endpoint}", params=p, timeout=30)
            return r.json()
        except Exception as e:
            return {"error": str(e)}

    def _post(self, endpoint: str, data: dict) -> dict:
        try:
            r = requests.post(
                f"{self.url}{endpoint}",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                json=data,
                timeout=30,
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}

    def health(self) -> dict:
        return self._get("/api/health")

    def recent_capsules(self, limit: int = 10) -> list:
        return self._get("/api/recent", {"limit": limit}).get("capsules", [])

    def search(self, query: str, limit: int = 5) -> list:
        return self._get("/api/search", {"q": query, "limit": limit}).get("capsules", [])

    def store(self, content: str, topic: str = "mind", keywords: list = None) -> dict:
        return self._post("/api/store", {
            "content": content,
            "topic": topic,
            "keywords": keywords or ["chronicle-mind"],
        })

    def inbox(self) -> dict:
        return self._get("/api/inbox")
