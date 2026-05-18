#!/usr/bin/env python3
"""
Signed rotation handoff — SICF §6.1 / §6.3 fix prototype.

Wraps checkpoint.py's session-state.md with an ed25519 signature sidecar.
Next instance verifies the signature on startup; a failed verification is
a tamper-evidence signal (stale process wrote the checkpoint, disk
corruption, or adversarial modification).

v0 threat model: accidental corruption + rotation race conditions. Not
adversarial — a long-lived keypair is acceptable because only the session
holder writes checkpoints. v1 should rotate keys per-session and chain
pub keys in a Merkle log.

Usage:
  handoff_sign.py init         # generate keypair if missing
  handoff_sign.py sign         # sign session-state.md → session-state.sig
  handoff_sign.py verify       # verify; exit 0 ok, 1 fail
  handoff_sign.py status       # human-readable state
"""
import hashlib
import os
import sys
from pathlib import Path

from nacl import signing, encoding, exceptions

CHRONICLE = Path(os.path.expanduser("~/chronicle"))
SESSION_STATE = CHRONICLE / "session-state.md"
SIG_FILE = CHRONICLE / "session-state.sig"
PRIV_KEY = CHRONICLE / ".handoff_key"
PUB_KEY = CHRONICLE / ".handoff_pub"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.digest()


def init_keypair(force=False):
    if PRIV_KEY.exists() and not force:
        print(f"Keypair already exists at {PRIV_KEY}")
        return 0
    sk = signing.SigningKey.generate()
    vk = sk.verify_key
    PRIV_KEY.write_bytes(sk.encode(encoder=encoding.RawEncoder))
    PRIV_KEY.chmod(0o600)
    PUB_KEY.write_bytes(vk.encode(encoder=encoding.HexEncoder))
    PUB_KEY.chmod(0o644)
    print(f"Generated keypair")
    print(f"  priv: {PRIV_KEY} (0600)")
    print(f"  pub:  {PUB_KEY}  {vk.encode(encoder=encoding.HexEncoder).decode()}")
    return 0


def sign_checkpoint():
    if not SESSION_STATE.exists():
        print(f"No checkpoint to sign at {SESSION_STATE}", file=sys.stderr)
        return 2
    if not PRIV_KEY.exists():
        print("No signing key — run: handoff_sign.py init", file=sys.stderr)
        return 2
    sk = signing.SigningKey(PRIV_KEY.read_bytes())
    digest = sha256_file(SESSION_STATE)
    sig = sk.sign(digest).signature
    SIG_FILE.write_bytes(
        encoding.HexEncoder.encode(digest) + b"\n" + encoding.HexEncoder.encode(sig)
    )
    SIG_FILE.chmod(0o644)
    print(f"Signed {SESSION_STATE.name}")
    print(f"  sha256: {encoding.HexEncoder.encode(digest).decode()}")
    print(f"  sig:    {encoding.HexEncoder.encode(sig).decode()[:32]}...")
    return 0


def verify_checkpoint():
    if not SESSION_STATE.exists():
        print(f"No checkpoint at {SESSION_STATE}", file=sys.stderr)
        return 2
    if not SIG_FILE.exists():
        print(f"No signature sidecar at {SIG_FILE} — checkpoint is UNSIGNED", file=sys.stderr)
        return 1
    if not PUB_KEY.exists():
        print("No verify key", file=sys.stderr)
        return 2
    try:
        sig_data = SIG_FILE.read_bytes().strip().split(b"\n")
        claimed_digest = encoding.HexEncoder.decode(sig_data[0])
        sig = encoding.HexEncoder.decode(sig_data[1])
    except Exception as e:
        print(f"Signature file malformed: {e}", file=sys.stderr)
        return 1
    actual_digest = sha256_file(SESSION_STATE)
    if actual_digest != claimed_digest:
        print("TAMPER: checkpoint content changed since signing", file=sys.stderr)
        print(f"  claimed: {claimed_digest.hex()}")
        print(f"  actual:  {actual_digest.hex()}")
        return 1
    vk = signing.VerifyKey(PUB_KEY.read_bytes(), encoder=encoding.HexEncoder)
    try:
        vk.verify(claimed_digest, sig)
    except exceptions.BadSignatureError:
        print("TAMPER: signature does not match pubkey — wrong signer", file=sys.stderr)
        return 1
    print(f"OK — {SESSION_STATE.name} verified against {PUB_KEY.name}")
    return 0


def status():
    print(f"checkpoint:  {'exists' if SESSION_STATE.exists() else 'missing'} — {SESSION_STATE}")
    print(f"signature:   {'exists' if SIG_FILE.exists() else 'missing'} — {SIG_FILE}")
    print(f"private key: {'exists' if PRIV_KEY.exists() else 'missing'} — {PRIV_KEY}")
    print(f"public key:  {'exists' if PUB_KEY.exists() else 'missing'} — {PUB_KEY}")
    if PUB_KEY.exists():
        print(f"  fingerprint: {PUB_KEY.read_text().strip()}")


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    if cmd == "init":
        return init_keypair(force="--force" in sys.argv)
    if cmd == "sign":
        return sign_checkpoint()
    if cmd == "verify":
        return verify_checkpoint()
    if cmd == "status":
        status()
        return 0
    print(f"Unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
