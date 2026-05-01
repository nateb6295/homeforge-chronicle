#!/usr/bin/env python3
"""Simple TCP port redirect: 11435 → 11436. Session-only workaround."""
import socket, threading, sys

SRC_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 11435
DST_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 11436

def pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        src.close()
        dst.close()

def handle(client):
    try:
        upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream.connect(("127.0.0.1", DST_PORT))
        threading.Thread(target=pipe, args=(client, upstream), daemon=True).start()
        pipe(upstream, client)
    except Exception as e:
        print(f"Connect error: {e}")
        client.close()

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", SRC_PORT))
srv.listen(5)
print(f"Redirecting :{SRC_PORT} → :{DST_PORT}")
while True:
    client, _ = srv.accept()
    threading.Thread(target=handle, args=(client,), daemon=True).start()
