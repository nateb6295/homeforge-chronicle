#!/usr/bin/env python3
"""opus_heartbeat.py — an ALIVE signal that does not run on the machine it watches.

WHY THIS EXISTS (Nate, 2026-08-24): "Discord was my only way to catch a crash or
an error that took YOU offline. it was pulling teeth to get a reliable ALIVE
check so i didnt miss silent failures or Oauth errors or the AGX crashing."

Every watchdog in Chronicle ran ON the AGX — health_alert.py, the sentinel,
gist_watchdog — so none of them could report their own death. The only signal
that reached Nate from outside was me posting to Discord, which is why he was
checking his phone at 00:38 on five hours of sleep.

This publishes a heartbeat to the MQTT broker on the Pi5. Home Assistant creates
a binary_sensor with expire_after, so THE PI decides we are dead when the
heartbeat stops. The AGX does not participate in noticing its own failure.

Catches: process death, AGX crash/hang, network partition, OAuth-wedged loops
         (the payload carries CCS version, which freezes if compression dies).
Does NOT catch: whole-house power/internet loss — Nate knows about those anyway.
"""
import json, os, socket, sqlite3, sys, time
import paho.mqtt.client as mqtt

BROKER   = os.environ.get("MQTT_HOST", "192.168.1.10")
PORT     = int(os.environ.get("MQTT_PORT", "1883"))
EXPIRE_S = 900                      # 15 min; publish every 5 -> 3 misses to alarm
DISC_T   = "homeassistant/binary_sensor/opus_alive/config"
STATE_T  = "chronicle/opus/heartbeat"
ATTR_T   = "chronicle/opus/heartbeat/attr"

def ccs_version():
    try:
        db = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db", timeout=5)
        v, u = db.execute("select version, updated_at from cognitive_state "
                          "order by rowid desc limit 1").fetchone()
        db.close()
        return v, round((time.time() - u) / 3600, 2)
    except Exception:
        return None, None

def main():
    v, age = ccs_version()
    attrs = {"host": socket.gethostname(), "ccs_version": v,
             "ccs_age_hours": age, "published": int(time.time())}
    c = mqtt.Client(client_id="opus-heartbeat")
    # Last Will: if this client dies uncleanly the BROKER publishes 'offline'
    c.will_set(STATE_T, "offline", retain=True)
    c.connect(BROKER, PORT, keepalive=30)
    c.publish(DISC_T, json.dumps({
        "name": "Opus Alive",
        "unique_id": "opus_alive",
        "state_topic": STATE_T,
        "json_attributes_topic": ATTR_T,
        "payload_on": "online",
        "payload_off": "offline",
        "device_class": "connectivity",
        "expire_after": EXPIRE_S,
        "device": {"identifiers": ["chronicle_opus"], "name": "Opus (AGX)"},
    }), retain=True)
    c.publish(ATTR_T, json.dumps(attrs), retain=True)
    c.publish(STATE_T, "online", retain=True)
    c.loop(timeout=2.0)
    c.disconnect()
    print(f"heartbeat published: ccs v{v}, {age}h old")

if __name__ == "__main__":
    main()
