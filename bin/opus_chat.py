#!/usr/bin/env python3
"""
Opus Chat — mobile-friendly two-way chat between Nate and Opus.
Runs on AGX, accessible via Tailscale or local network.
Messages persist in a JSON file that Opus polls.
"""

import os
import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs
import cgi

CHAT_FILE = os.path.expanduser("~/chronicle/data/opus_chat.json")
PORT = 8078
MAX_MESSAGES = 200

os.makedirs(os.path.dirname(CHAT_FILE), exist_ok=True)


def _load_messages():
    if os.path.exists(CHAT_FILE):
        try:
            with open(CHAT_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


def _save_messages(msgs):
    msgs = msgs[-MAX_MESSAGES:]
    with open(CHAT_FILE, 'w') as f:
        json.dump(msgs, f, indent=2)


def _add_message(author, content):
    msgs = _load_messages()
    msgs.append({
        "author": author,
        "content": content,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "epoch": int(time.time()),
        "read": False if author == "nate" else True
    })
    _save_messages(msgs)
    return msgs


HTML_PAGE = """<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="Opus">
<title>Opus</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, sans-serif; background: #0d1117; color: #e0e0e0;
         height: 100vh; display: flex; flex-direction: column; }
  .header { background: #161b22; padding: 12px 16px; border-bottom: 1px solid #30363d;
            display: flex; align-items: center; gap: 10px; }
  .header .dot { width: 10px; height: 10px; border-radius: 50%; background: #7c83ff; }
  .header h1 { font-size: 18px; color: #7c83ff; font-weight: 600; }
  .header .sub { font-size: 12px; color: #666; margin-left: auto; }
  .messages { flex: 1; overflow-y: auto; padding: 12px 16px; display: flex;
              flex-direction: column; gap: 8px; }
  .msg { max-width: 85%; padding: 10px 14px; border-radius: 16px; font-size: 15px;
         line-height: 1.4; word-wrap: break-word; white-space: pre-wrap; }
  .msg.opus { background: #1c2333; border-bottom-left-radius: 4px; align-self: flex-start;
              border: 1px solid #30363d; }
  .msg.nate { background: #7c83ff; color: #fff; border-bottom-right-radius: 4px;
              align-self: flex-end; }
  .msg .meta { font-size: 11px; color: #666; margin-top: 4px; }
  .msg.nate .meta { color: rgba(255,255,255,0.6); }
  .input-bar { background: #161b22; padding: 10px 12px; border-top: 1px solid #30363d;
               display: flex; gap: 8px; }
  .input-bar textarea { flex: 1; background: #0d1117; color: #e0e0e0; border: 1px solid #30363d;
                        border-radius: 20px; padding: 10px 16px; font-size: 15px; resize: none;
                        font-family: inherit; min-height: 40px; max-height: 120px; }
  .input-bar textarea:focus { outline: none; border-color: #7c83ff; }
  .input-bar button { background: #7c83ff; color: white; border: none; border-radius: 50%;
                      width: 40px; height: 40px; font-size: 18px; cursor: pointer;
                      display: flex; align-items: center; justify-content: center; }
  .input-bar button:active { background: #6366f1; }
  .empty { color: #444; text-align: center; margin-top: 40px; font-size: 14px; }
</style>
</head>
<body>
<div class="header">
  <div class="dot"></div>
  <h1>Opus</h1>
  <span class="sub" id="status">connected</span>
</div>

<div class="messages" id="messages">
  <div class="empty" id="emptyMsg">Loading...</div>
</div>

<div class="input-bar">
  <textarea id="input" rows="1" placeholder="Message Opus..."
    onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();send();}"></textarea>
  <button onclick="send()">&#9654;</button>
</div>

<script>
const messagesDiv = document.getElementById('messages');
const input = document.getElementById('input');
const emptyMsg = document.getElementById('emptyMsg');
const status = document.getElementById('status');
let lastCount = 0;
let pollInterval = null;
let notifGranted = false;

if ('Notification' in window) {
  if (Notification.permission === 'granted') notifGranted = true;
  else if (Notification.permission !== 'denied') {
    Notification.requestPermission().then(p => { notifGranted = p === 'granted'; });
  }
}

function notify(msg) {
  if (!notifGranted || document.hasFocus()) return;
  try { new Notification('Opus', { body: msg.slice(0, 100), icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><circle cx="50" cy="50" r="40" fill="%237c83ff"/></svg>' }); } catch(e) {}
}

function renderMessages(msgs) {
  if (!msgs.length) { emptyMsg.style.display = 'block'; emptyMsg.textContent = 'No messages yet'; return; }
  emptyMsg.style.display = 'none';

  if (msgs.length !== lastCount) {
    const newOpusMsgs = msgs.slice(lastCount).filter(m => m.author === 'opus');
    messagesDiv.innerHTML = '';
    msgs.forEach(m => {
      const div = document.createElement('div');
      div.className = 'msg ' + m.author;
      div.innerHTML = escapeHtml(m.content) + '<div class="meta">' + m.timestamp + '</div>';
      messagesDiv.appendChild(div);
    });
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
    if (lastCount > 0) newOpusMsgs.forEach(m => notify(m.content));
    lastCount = msgs.length;
  }
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function poll() {
  fetch('/messages')
    .then(r => r.json())
    .then(d => { renderMessages(d.messages); status.textContent = 'connected'; })
    .catch(() => { status.textContent = 'offline'; });
}

function send() {
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  input.style.height = 'auto';

  fetch('/send', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({content: text})
  })
  .then(r => r.json())
  .then(d => { if (d.ok) poll(); });
}

input.addEventListener('input', function() {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 120) + 'px';
});

poll();
pollInterval = setInterval(poll, 3000);
</script>
</body>
</html>"""


class ChatHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode())
        elif self.path == '/messages':
            msgs = _load_messages()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"messages": msgs[-50:]}).encode())
        elif self.path == '/unread':
            msgs = _load_messages()
            unread = [m for m in msgs if m.get("author") == "nate" and not m.get("read")]
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"unread": unread, "count": len(unread)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/send':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            content = body.get('content', '').strip()
            if content:
                _add_message("nate", content)
                self._json(200, {"ok": True})
            else:
                self._json(400, {"ok": False, "error": "empty"})
        elif self.path == '/opus':
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
            content = body.get('content', '').strip()
            if content:
                _add_message("opus", content)
                self._json(200, {"ok": True})
            else:
                self._json(400, {"ok": False, "error": "empty"})
        elif self.path == '/mark_read':
            msgs = _load_messages()
            for m in msgs:
                if m.get("author") == "nate":
                    m["read"] = True
            _save_messages(msgs)
            self._json(200, {"ok": True})
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        pass


if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', PORT), ChatHandler)
    print(f"Opus Chat running on http://0.0.0.0:{PORT}")
    print(f"Messages stored at: {CHAT_FILE}")
    server.serve_forever()
