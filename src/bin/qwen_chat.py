#!/usr/bin/env python3
"""Chronicle Chat — Mobile-friendly web UI for chatting with local Ollama models on AGX."""
import json
import requests
from flask import Flask, request, Response, stream_with_context, render_template_string

OLLAMA_URL = "http://localhost:11434"
DEFAULT_MODEL = "qwen3:8b"

app = Flask(__name__)

HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>Chronicle Chat</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body {
    font-family: -apple-system, system-ui, 'Segoe UI', sans-serif;
    background: #1a1a2e; color: #e0e0e0;
    height: 100%; height: 100dvh;
    overflow: hidden;
  }
  body {
    display: flex; flex-direction: column;
    padding-top: env(safe-area-inset-top);
    padding-bottom: env(safe-area-inset-bottom);
  }

  /* Header — compact on mobile */
  .header {
    padding: 10px 12px;
    background: #16213e;
    border-bottom: 1px solid #0f3460;
    display: flex; align-items: center; gap: 8px;
    flex-shrink: 0;
  }
  .header h1 { font-size: 16px; color: #e94560; white-space: nowrap; }
  .header select {
    background: #0f3460; color: #e0e0e0;
    border: 1px solid #533483;
    padding: 8px 6px; border-radius: 6px;
    font-size: 13px; max-width: 140px;
    -webkit-appearance: none;
  }
  .header .status {
    margin-left: auto; font-size: 11px; color: #888;
    white-space: nowrap;
  }
  .header-actions {
    display: flex; gap: 6px; align-items: center;
  }
  .clear-btn {
    background: transparent; color: #888;
    border: 1px solid #533483; border-radius: 6px;
    padding: 8px 10px; cursor: pointer; font-size: 12px;
    min-width: 44px; min-height: 44px;
    display: flex; align-items: center; justify-content: center;
  }
  .clear-btn:hover, .clear-btn:active { color: #e94560; border-color: #e94560; }

  /* Chat area */
  .chat {
    flex: 1; overflow-y: auto; overflow-x: hidden;
    padding: 12px;
    display: flex; flex-direction: column; gap: 12px;
    -webkit-overflow-scrolling: touch;
  }
  .msg {
    max-width: 90%; padding: 10px 14px;
    border-radius: 16px; line-height: 1.5;
    white-space: pre-wrap; word-wrap: break-word;
    font-size: 15px;
  }
  .msg.user {
    align-self: flex-end;
    background: #0f3460;
    border-bottom-right-radius: 4px;
  }
  .msg.assistant {
    align-self: flex-start;
    background: #16213e;
    border: 1px solid #533483;
    border-bottom-left-radius: 4px;
  }
  .msg.assistant .model-tag { font-size: 11px; color: #e94560; margin-bottom: 4px; }
  .thinking-indicator { color: #e94560; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

  /* Input area — fixed at bottom, safe-area aware */
  .input-area {
    padding: 10px 12px;
    padding-bottom: calc(10px + env(safe-area-inset-bottom));
    background: #16213e;
    border-top: 1px solid #0f3460;
    display: flex; gap: 8px; align-items: flex-end;
    flex-shrink: 0;
  }
  .input-area textarea {
    flex: 1;
    background: #1a1a2e; color: #e0e0e0;
    border: 1px solid #533483; border-radius: 20px;
    padding: 10px 16px;
    font-size: 16px; /* prevents iOS zoom */
    font-family: inherit;
    resize: none;
    min-height: 44px; max-height: 120px;
    line-height: 1.4;
  }
  .input-area textarea:focus { outline: none; border-color: #e94560; }
  .input-area button {
    background: #e94560; color: white;
    border: none; border-radius: 50%;
    width: 44px; height: 44px;
    font-size: 18px; cursor: pointer;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0;
  }
  .input-area button:hover { background: #c73e54; }
  .input-area button:disabled { background: #555; cursor: not-allowed; }

  /* Empty state */
  .empty-state {
    flex: 1; display: flex; flex-direction: column;
    align-items: center; justify-content: center;
    color: #555; text-align: center; padding: 20px;
  }
  .empty-state h2 { color: #e94560; font-size: 20px; margin-bottom: 8px; }
  .empty-state p { font-size: 14px; max-width: 280px; }

  /* Desktop tweaks */
  @media (min-width: 768px) {
    .header { padding: 12px 20px; }
    .chat { padding: 20px; gap: 16px; }
    .msg { max-width: 70%; font-size: 15px; padding: 12px 16px; }
    .input-area { padding: 16px 20px; gap: 10px; }
    .input-area textarea { border-radius: 12px; }
    .input-area button { border-radius: 8px; width: auto; padding: 12px 20px; }
    .input-area button::after { content: ' Send'; }
  }
</style>
</head>
<body>
<div class="header">
  <h1>Chronicle</h1>
  <select id="model">{% for m in models %}<option value="{{m}}" {% if m == default %}selected{% endif %}>{{m}}</option>{% endfor %}</select>
  <div class="header-actions">
    <button class="clear-btn" onclick="clearChat()" title="Clear chat">&#x2715;</button>
  </div>
  <span class="status">AGX 64GB</span>
</div>
<div class="chat" id="chat">
  <div class="empty-state" id="emptyState">
    <h2>Chronicle Chat</h2>
    <p>Qwen3-8B &middot; Sovereign inference on AGX Orin. Your thoughts stay on your hardware.</p>
  </div>
</div>
<div class="input-area">
  <textarea id="input" placeholder="Message Chronicle..." rows="1"
    onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();send()}"
    enterkeyhint="send"></textarea>
  <button id="sendBtn" onclick="send()" aria-label="Send">&#x25B6;</button>
</div>
<script>
let messages = [];
let generating = false;

function hideEmpty() {
  const e = document.getElementById('emptyState');
  if (e) e.remove();
}

function addMsg(role, content, model) {
  hideEmpty();
  const chat = document.getElementById('chat');
  const div = document.createElement('div');
  div.className = 'msg ' + role;
  if (role === 'assistant' && model) {
    div.innerHTML = '<div class="model-tag">' + model + '</div><span class="content"></span>';
  } else {
    div.innerHTML = '<span class="content"></span>';
  }
  div.querySelector('.content').textContent = content;
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
  return div;
}

async function send() {
  if (generating) return;
  const input = document.getElementById('input');
  const text = input.value.trim();
  if (!text) return;

  const model = document.getElementById('model').value;

  messages.push({role: 'user', content: text});
  addMsg('user', text);
  input.value = '';
  input.style.height = 'auto';

  generating = true;
  document.getElementById('sendBtn').disabled = true;
  const assistantDiv = addMsg('assistant', '', model);
  const contentSpan = assistantDiv.querySelector('.content');
  contentSpan.innerHTML = '<span class="thinking-indicator">Thinking...</span>';

  try {
    const resp = await fetch('/chat', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model: model, messages: messages})
    });

    if (!resp.ok) throw new Error('Server error: ' + resp.status);

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let fullResponse = '';
    let firstContent = true;
    let thinkingStarted = false;

    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, {stream: true});
      for (const line of chunk.split('\\n')) {
        if (!line.trim()) continue;
        try {
          const data = JSON.parse(line);
          if (data.error) {
            contentSpan.textContent = 'Error: ' + data.error;
            break;
          }
          if (data.message && data.message.thinking) {
            if (!thinkingStarted) {
              thinkingStarted = true;
              contentSpan.innerHTML = '<span class="thinking-indicator">Reasoning...</span>';
            }
            continue;
          }
          if (data.message && data.message.content) {
            const token = data.message.content;
            if (firstContent) {
              contentSpan.textContent = '';
              firstContent = false;
            }
            fullResponse += token;
            contentSpan.textContent += token;
            document.getElementById('chat').scrollTop = document.getElementById('chat').scrollHeight;
          }
        } catch(e) {}
      }
    }
    if (firstContent) contentSpan.textContent = fullResponse || '(no response)';
    messages.push({role: 'assistant', content: fullResponse});
  } catch(e) {
    contentSpan.textContent = 'Error: ' + e.message;
  }

  generating = false;
  document.getElementById('sendBtn').disabled = false;
  input.focus();
}

function clearChat() {
  messages = [];
  document.getElementById('chat').innerHTML =
    '<div class="empty-state" id="emptyState"><h2>Chronicle Chat</h2><p>Qwen3-8B &middot; Sovereign inference on AGX Orin. Your thoughts stay on your hardware.</p></div>';
}

// Auto-resize textarea
document.getElementById('input').addEventListener('input', function() {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 120) + 'px';
});

// Handle mobile keyboard resize
const viewport = window.visualViewport;
if (viewport) {
  viewport.addEventListener('resize', () => {
    document.body.style.height = viewport.height + 'px';
    const chat = document.getElementById('chat');
    chat.scrollTop = chat.scrollHeight;
  });
}
</script>
</body>
</html>"""

@app.route("/")
def index():
    # Get available models — always put default first
    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        # Ensure default model is first in list
        if DEFAULT_MODEL in models:
            models.remove(DEFAULT_MODEL)
            models.insert(0, DEFAULT_MODEL)
    except Exception:
        models = [DEFAULT_MODEL]
    return render_template_string(HTML, models=models, default=DEFAULT_MODEL)

@app.route("/chat", methods=["POST"])
def chat():
    data = request.json
    model = data.get("model", DEFAULT_MODEL)
    msgs = data.get("messages", [])

    def generate():
        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": model,
                    "messages": msgs,
                    "stream": True,
                    "think": False,
                    "options": {
                        "num_ctx": 8192,
                    },
                },
                stream=True,
                timeout=600,
            )
            r.raise_for_status()
            for line in r.iter_lines():
                if line:
                    yield line.decode("utf-8") + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    return Response(stream_with_context(generate()), mimetype="text/plain")

@app.route("/health")
def health():
    """Health check endpoint."""
    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        return json.dumps({"status": "ok", "models": models}), 200
    except Exception as e:
        return json.dumps({"status": "error", "error": str(e)}), 503

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
