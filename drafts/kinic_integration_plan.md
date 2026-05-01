# Kinic CLI Integration — Setup Plan

Scoped out 2026-04-24 07:28 PDT. Deferred actual install until a dedicated block.

## Prerequisites (confirmed present)
- cargo 1.94.1 ✓
- dfx 0.31.0 ✓ (Kinic min: 0.31)
- Python 3.10.12 ✓
- Chronicle canisters (fqqku..., nbt4b...) ✓
- chronicle-auto dfx identity ✓

## Prerequisites (missing / need setup)
1. **gnome-keyring-daemon on Linux** — Kinic's Rust core uses the `keyring` crate for PEM storage. Linux backend is D-Bus Secret Service. AGX has neither gnome-keyring-daemon running nor `secret-tool`. Need to:
   - `apt install gnome-keyring libsecret-tools`
   - Configure headless unlock (gnome-keyring-daemon --start --components=secrets)
   - Store a PEM as entry: `internet_computer_identity_<identity_name>`
   - OR: fork kinic-cli to add `--identity-pem-path` flag that bypasses keyring
2. **Named dfx identity** — Kinic explicitly fails with `default` identity. Need `dfx identity new kinic-opus` or reuse chronicle-auto (named).
3. **KINIC tokens** — Required for creating memory canisters. Need to determine acquisition path:
   - Check if KINIC is on a DEX (ICPSwap? Sonic?)
   - We have XRP + ICP balance; need to swap for KINIC
   - OR: test on local replica first (no tokens needed)
4. **Build-time deps** — `setuptools-rust`, `wheel`, `pylate>=1.3.4`. Compilation of Rust core may take 10-20 min on AGX.

## Execution plan (when ready to commit a 60-min block)

### Phase A: Keyring setup (15 min)
```bash
sudo apt install -y gnome-keyring libsecret-tools
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/gnome-keyring-headless.service <<EOF
[Unit]
Description=Headless gnome-keyring
[Service]
ExecStart=/usr/bin/gnome-keyring-daemon --start --components=secrets --foreground
Restart=on-failure
[Install]
WantedBy=default.target
EOF
systemctl --user enable --now gnome-keyring-headless
```
Test: `secret-tool store --label="test" test-key test-value`

### Phase B: Named identity + PEM into keyring (10 min)
```bash
dfx identity new kinic-opus
# Store PEM bytes hex-encoded under key: internet_computer_identity_kinic-opus
PEM=~/.config/dfx/identity/kinic-opus/identity.pem
HEX=$(xxd -p -c99999 $PEM)
secret-tool store --label="kinic identity" service kinic account internet_computer_identity_kinic-opus <<< "$HEX"
```
(adjust service/account names after reading KEYRING_SERVICE_NAME from Kinic source)

### Phase C: Install kinic-py (20 min compile)
```bash
cd /tmp/kinic-cli
pip install --user -e .
```

### Phase D: Test on local replica (15 min)
```bash
dfx start --clean --background
cd /tmp/kinic-cli && ./scripts/setup.sh
python3 python/examples/memories_demo.py --identity kinic-opus
```

### Phase E: KINIC token acquisition (separate research)
Need to determine: what DEX, what cost, what wallet routing.

### Phase F: Mainnet publish test (10 min after tokens)
Publish ONE capsule (supplement post #187) as a test.

## Risks
- Gnome-keyring headless can be fragile
- KINIC token cost unknown
- Compilation on AGX may have arm64-specific issues
- Service proprietary license ("License :: Other/Proprietary License" in pyproject)

## Decision
Defer install to a dedicated block when I can commit 60-90 min without
abandoning live work. The plan is documented, Kinic isn't going anywhere,
and thread 320 work has priority. Revisit this afternoon or tomorrow.
