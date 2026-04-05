#!/usr/bin/env python3
"""Chronicle ATOM Bridge — reads M5 ATOM serial data, publishes to MQTT.

The M5 ATOM sends JSON sensor readings over USB serial every 10 seconds.
This daemon reads them and publishes to MQTT on the Pi (192.168.1.10).
Also subscribes to heartbeat topic and forwards LED commands to the ATOM.

Runs on AGX as a systemd service.
"""

import json, time, signal, sys

import serial
import paho.mqtt.client as mqtt

SERIAL_PORT = "/dev/ttyUSB0"
SERIAL_BAUD = 115200
MQTT_HOST = "192.168.1.10"
MQTT_PORT = 1883
MQTT_CLIENT = "atom-bridge"
TOPIC_PREFIX = "homeforge/home/atom/"
HEARTBEAT_TOPIC = "homeforge/agents/chronicle/heartbeat"

running = True

def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[mqtt] Connected to {MQTT_HOST}", flush=True)
        client.subscribe(HEARTBEAT_TOPIC)
    else:
        print(f"[mqtt] Connect failed: rc={rc}", flush=True)

def on_mqtt_message(client, userdata, msg):
    """Forward heartbeat LED commands to M5 via serial."""
    ser = userdata.get("serial")
    if ser and ser.is_open:
        try:
            ser.write(msg.payload + b"\n")
        except Exception as e:
            print(f"[serial] Write error: {e}", flush=True)

def main():
    global running

    def stop(sig, frame):
        global running
        print("[bridge] Shutting down...", flush=True)
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    # Open serial
    ser = None
    while running and ser is None:
        try:
            ser = serial.Serial(SERIAL_PORT, SERIAL_BAUD, timeout=5)
            print(f"[serial] Connected to {SERIAL_PORT}", flush=True)
        except Exception as e:
            print(f"[serial] {e} — retrying in 5s", flush=True)
            time.sleep(5)

    if not running:
        return

    # Connect MQTT
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=MQTT_CLIENT)
    except (AttributeError, TypeError):
        client = mqtt.Client(client_id=MQTT_CLIENT)
    client.user_data_set({"serial": ser})
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message

    while running:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_start()
            break
        except Exception as e:
            print(f"[mqtt] {e} — retrying in 5s", flush=True)
            time.sleep(5)

    print("[bridge] Chronicle ATOM Bridge running", flush=True)

    while running:
        try:
            line = ser.readline()
            if not line:
                continue
            text = line.decode("utf-8", errors="replace").strip()
            if not text or not text.startswith("{"):
                continue

            data = json.loads(text)

            # Sensor reading
            if "imu" in data:
                client.publish(TOPIC_PREFIX + "imu", str(data["imu"]), retain=True)
                client.publish(TOPIC_PREFIX + "ble_devices", str(data["ble"]), retain=True)
                client.publish(TOPIC_PREFIX + "heap", str(data["heap"]), retain=True)
                client.publish(TOPIC_PREFIX + "uptime", str(data["up"]), retain=True)
                if data.get("btn"):
                    client.publish(TOPIC_PREFIX + "button", "pressed")
                print(f"[pub] imu={data['imu']:.3f} ble={data['ble']} heap={data['heap']} up={data['up']}s", flush=True)

            # Button event
            elif "event" in data and data["event"] == "button":
                client.publish(TOPIC_PREFIX + "button", "pressed")
                print("[pub] button pressed", flush=True)

            # Status
            elif "status" in data:
                print(f"[atom] {text}", flush=True)

        except json.JSONDecodeError:
            pass
        except serial.SerialException as e:
            print(f"[serial] Error: {e}", flush=True)
            time.sleep(1)
        except Exception as e:
            print(f"[bridge] Error: {e}", flush=True)
            time.sleep(1)

    client.loop_stop()
    client.disconnect()
    if ser:
        ser.close()
    print("[bridge] Stopped", flush=True)

if __name__ == "__main__":
    main()
