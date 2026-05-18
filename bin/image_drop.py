#!/usr/bin/env python3
"""
Image drop server — local network upload endpoint.
Nate opens this on his phone, drops an image, Opus can read it.
"""

import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import cgi
import json

UPLOAD_DIR = os.path.expanduser("~/chronicle/uploads")
PORT = 8077
os.makedirs(UPLOAD_DIR, exist_ok=True)

HTML_PAGE = """<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Drop for Opus</title>
<style>
  body { font-family: -apple-system, sans-serif; background: #1a1a2e; color: #e0e0e0;
         display: flex; flex-direction: column; align-items: center; padding: 20px; }
  h1 { color: #7c83ff; margin-bottom: 5px; }
  .sub { color: #888; margin-bottom: 30px; font-size: 14px; }
  .drop-zone { width: 90%; max-width: 400px; min-height: 200px; border: 2px dashed #7c83ff;
                border-radius: 12px; display: flex; flex-direction: column; align-items: center;
                justify-content: center; padding: 20px; cursor: pointer; transition: 0.2s; }
  .drop-zone:hover, .drop-zone.drag-over { border-color: #fff; background: rgba(124,131,255,0.1); }
  .drop-zone p { font-size: 18px; margin: 10px 0; }
  input[type="file"] { display: none; }
  .preview { max-width: 100%; max-height: 300px; border-radius: 8px; margin-top: 15px; }
  .status { margin-top: 15px; font-size: 14px; }
  .success { color: #4caf50; }
  .error { color: #f44336; }
  .btn { background: #7c83ff; color: white; border: none; padding: 12px 30px;
         border-radius: 8px; font-size: 16px; cursor: pointer; margin-top: 15px; }
  .btn:disabled { opacity: 0.5; }
  .recent { width: 90%; max-width: 400px; margin-top: 30px; }
  .recent h3 { color: #7c83ff; }
  .recent-item { font-size: 13px; color: #888; padding: 4px 0; }
</style>
</head>
<body>
<h1>Drop for Opus</h1>
<p class="sub">Images land in ~/chronicle/uploads/</p>

<div class="drop-zone" id="dropZone" onclick="fileInput.click()">
  <p>Tap or drop image here</p>
  <input type="file" id="fileInput" accept="image/*" capture="environment">
  <img id="preview" class="preview" style="display:none">
</div>

<button class="btn" id="sendBtn" onclick="upload()" disabled>Send to Opus</button>
<div class="status" id="status"></div>

<div class="recent" id="recentBox"></div>

<script>
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const preview = document.getElementById('preview');
const sendBtn = document.getElementById('sendBtn');
const status = document.getElementById('status');
let selectedFile = null;

fileInput.addEventListener('change', (e) => { handleFile(e.target.files[0]); });

dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.classList.add('drag-over'); });
dropZone.addEventListener('dragleave', () => { dropZone.classList.remove('drag-over'); });
dropZone.addEventListener('drop', (e) => {
  e.preventDefault(); dropZone.classList.remove('drag-over');
  if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
});

function handleFile(file) {
  if (!file || !file.type.startsWith('image/')) { status.innerHTML = '<span class="error">Not an image</span>'; return; }
  selectedFile = file;
  const reader = new FileReader();
  reader.onload = (e) => { preview.src = e.target.result; preview.style.display = 'block'; };
  reader.readAsDataURL(file);
  sendBtn.disabled = false;
  status.textContent = file.name + ' (' + (file.size/1024).toFixed(0) + ' KB)';
}

function upload() {
  if (!selectedFile) return;
  sendBtn.disabled = true;
  status.textContent = 'Uploading...';
  const fd = new FormData();
  fd.append('image', selectedFile);
  fetch('/upload', { method: 'POST', body: fd })
    .then(r => r.json())
    .then(d => {
      if (d.ok) {
        status.innerHTML = '<span class="success">Sent! ' + d.path + '</span>';
        preview.style.display = 'none';
        selectedFile = null;
        loadRecent();
      } else {
        status.innerHTML = '<span class="error">' + d.error + '</span>';
        sendBtn.disabled = false;
      }
    })
    .catch(e => { status.innerHTML = '<span class="error">Failed: ' + e + '</span>'; sendBtn.disabled = false; });
}

function loadRecent() {
  fetch('/recent').then(r => r.json()).then(d => {
    const box = document.getElementById('recentBox');
    if (d.files.length) {
      box.innerHTML = '<h3>Recent drops</h3>' + d.files.map(f =>
        '<div class="recent-item">' + f + '</div>').join('');
    }
  });
}
loadRecent();
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode())
        elif self.path == '/recent':
            files = sorted(os.listdir(UPLOAD_DIR), reverse=True)[:10]
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"files": files}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != '/upload':
            self.send_response(404)
            self.end_headers()
            return

        content_type = self.headers.get('Content-Type', '')
        if 'multipart/form-data' not in content_type:
            self._json_response(400, {"ok": False, "error": "Not multipart"})
            return

        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': content_type}
        )

        file_item = form['image']
        if not file_item.filename:
            self._json_response(400, {"ok": False, "error": "No file"})
            return

        ext = os.path.splitext(file_item.filename)[1] or '.png'
        ts = time.strftime('%Y%m%d_%H%M%S')
        filename = f"drop_{ts}{ext}"
        filepath = os.path.join(UPLOAD_DIR, filename)

        with open(filepath, 'wb') as f:
            f.write(file_item.file.read())

        self._json_response(200, {"ok": True, "path": filepath, "filename": filename})

    def _json_response(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        pass


if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f"Image drop server running on http://0.0.0.0:{PORT}")
    print(f"Uploads go to: {UPLOAD_DIR}")
    server.serve_forever()
