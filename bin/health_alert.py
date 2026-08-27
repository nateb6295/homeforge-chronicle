#!/usr/bin/env python3
"""Health alert — silent unless something breaks.

Checks services, CCS freshness, operator silence ceiling, disk headroom.
Prints nothing if everything is OK. Posts to #operator only on problems.
Designed to run from a background cron so Opus doesn't have to poll.

Usage:
  python3 health_alert.py          # check and alert if needed
  python3 health_alert.py --dry    # check and print, don't post
  python3 health_alert.py --force  # always print status even if OK
"""

import argparse
import json
import re
import os
import urllib.request
import datetime
import subprocess
import sys
import time
import datetime as _dt

SERVICES = [
    "chronicle-sentinel",
    "chronicle-engine",
    "chronicle-hal",
    # Added Aug 23. These two are the DELIVERY PATH for Nate's messages and the
    # capture alerts -- chatwatcher pushes [NATE]/[CHAT] into the session,
    # capture-watch pushes capture alerts. Neither was monitored. The
    # discord-poll cron had been acting as an accidental fallback for them,
    # costing ~12 "no Nate messages" checks a day to cover a failure nobody
    # was watching for. Monitor the mechanism, drop the polling.
    "chronicle-chatwatcher",
    "chronicle-capture-watch",
    # Added Aug 24. ccs-adaptive is THE PERSISTENCE MECHANISM and was never
    # monitored -- 12 chronicle services exist and only 5 were in this list.
    "chronicle-ccs-adaptive",
]

# Services that log on a PERIOD. For these, silence is failure, and
# `systemctl is-active` cannot see it -- it returns "active" for a process that
# is running and failing every operation.
#
# Found Aug 24 via a capture (@lionellevine on refusing to update on epistemic
# inferiors). chronicle-lfm-sensor was "active", had logged "Score error: timed
# out" once a minute, then went silent for NINE HOURS. Nothing noticed, because
# nothing watched it, and the one check that would have covered it reports
# liveness rather than function.
#
# Event-driven services are deliberately NOT here: silence is their normal.
PERIODIC = {
    # chronicle-lfm-sensor is DELIBERATELY ABSENT. Added 2026-08-25, removed the
    # same morning after it generated a false positive — and after I discovered
    # my original "hang" diagnosis was WRONG.
    #
    # It logs only when it has messages to SCORE. #threads had no new messages
    # for 769 minutes, so it correctly logged nothing. It is EVENT-DRIVEN, and
    # the comment three lines above this one says event-driven services must not
    # be in this dict. I wrote that rule and then broke it within the hour.
    # History, kept because it explains the gap in this file:
    # It was REMOVED from the watch and STOPPED on 2026-08-24 ~22:10, after it
    # hung a SECOND time 33 minutes past a restart. Leaving it to alert every
    # 30 min all night would repeat today's other mistake — I already spammed
    # Nate about a condition he had acknowledged. A stopped service is an
    # HONEST state; a hung one reports green and lies. Diagnosis and fix are
    # queued in data/due.jsonl. Re-add this line when it is repaired.
    "chronicle-ccs-adaptive": 15,
    "chronicle-engine": 60,
    "chronicle-hal": 30,
    # PARTIAL COVERAGE, stated honestly: loquwen is a timer-driven oneshot, so a
    # CRASHING pulse still writes a traceback and would look "alive" to this
    # check. What this catches is the TIMER stopping. Her generation failures are
    # covered separately by the explicit stderr line added to loquwen_pulse.py.
    "chronicle-loquwen": 25,
}

CCS_MAX_AGE_MIN = 300
OPERATOR_SILENCE_CEILING_MIN = 90
OVERNIGHT_SILENCE_CEILING_MIN = 180

# (mount, minimum free GB). Root is a 57G eMMC — it fills quietly.
DISK_WATCH = [("/", 5.0), ("/mnt/hdd", 20.0)]
BIO_MAX_AGE_H = 30.0   # MEASURED Aug 23, see check_bio_feed docstring. Was 12.0,
                       # which was an assumption and sat exactly on the daily trough.
ALERT_COOLDOWN_H = 6.0
ALERT_STATE = os.path.expanduser("~/chronicle/data/health_alert_state.json")
RUNAWAY_LOG_MB = 500


def check_services():
    problems = []
    for svc in SERVICES:
        try:
            result = subprocess.run(
                ["systemctl", "--user", "is-active", svc],
                capture_output=True, text=True, timeout=5,
            )
            if result.stdout.strip() != "active":
                problems.append(f"{svc}: {result.stdout.strip()}")
        except Exception as e:
            problems.append(f"{svc}: check failed ({e})")
    return problems


CCS_MAX_VERSION_GAP_H = 4.75   # 3h floor + 4h ceiling per F160, plus slack
CCS_VERSION_STATE = os.path.expanduser("~/chronicle/data/ccs_version_watch.json")


def check_ccs_freshness():
    """Has a REAL compression happened, not just a touch?

    Aug 23: the old version of this read cognitive_state.updated_at -- which
    ccs_touch.py bumps every 10 minutes from crontab without doing any
    compression at all. So this alarm could never fire. CLAUDE.md calls a
    >6h compression gap "a missing heartbeat"; the thing meant to detect it
    was structurally incapable of doing so, for the same reason the bio-feed
    monitor was: THE MONITOR READ A FIELD THE CHEAP PATH ALSO WRITES.

    cognitive_state.version only advances in brain_compress. Watch that.
    updated_at is still reported, but as context, never as the trigger.
    """
    try:
        import sqlite3
        db = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db")
        row = db.execute(
            "SELECT version, updated_at FROM cognitive_state ORDER BY id DESC LIMIT 1"
        ).fetchone()
        db.close()
        if not row:
            return "CCS: no state found"
        version, updated_at = row[0], row[1]
        now = time.time()

        try:
            st = json.load(open(CCS_VERSION_STATE))
        except Exception:
            st = {}
        if st.get("version") != version:
            json.dump({"version": version, "seen_at": now},
                      open(CCS_VERSION_STATE, "w"))
            return None

        gap_h = (now - st.get("seen_at", now)) / 3600.0
        if gap_h > CCS_MAX_VERSION_GAP_H:
            touch_age_min = (now - updated_at) / 60
            return (f"CCS: version stuck at v{version} for {gap_h:.1f}h "
                    f"(ceiling {CCS_MAX_VERSION_GAP_H}h) — compressions are "
                    f"FAILING, not merely late. updated_at is only "
                    f"{touch_age_min:.0f}min old because ccs_touch keeps "
                    f"bumping it. Check ccs-adaptive.log for section-check "
                    f"failures (v5 requires ## UNFINISHED); revert with "
                    f"data/v5_prereg.md if that is the cause.")
    except Exception as e:
        return f"CCS: check failed ({e})"
    return None


def check_operator_silence():
    try:
        import sqlite3
        db = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db")
        row = db.execute(
            "SELECT created_at FROM activity_feed "
            "WHERE source LIKE '%operator%' OR source LIKE '%opus%' "
            "ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        db.close()
        if not row:
            return None
        age_min = (time.time() - row[0]) / 60
        hour = time.localtime().tm_hour
        ceiling = OVERNIGHT_SILENCE_CEILING_MIN if hour >= 22 or hour < 4 else OPERATOR_SILENCE_CEILING_MIN
        if age_min > ceiling:
            return f"Silence: {age_min:.0f}min since last post (ceiling={ceiling}min)"
    except Exception:
        pass
    return None


def diagnose_bio_path():
    """Which LINK in the biometrics chain is broken?

    Added Aug 23 after Nate said: "it's through TAILSCALE so a few things need
    to always be working." My outage diagnosis that morning tested
    192.168.1.70:8678 and declared the receiver healthy. The phone does not use
    that address. I verified a path that is not the path, and got the right
    answer by luck.

    The real chain, in order, each link able to fail independently:
        tailscaled up -> AGX online in tailnet -> phone present in tailnet
        -> 8678 answering ON THE TAILSCALE IP -> phone export actually running

    Only called when the feed already looks broken, so the normal pulse stays
    cheap and the diagnosis arrives attached to the alert that needs it.
    """
    import subprocess, json as _json

    def sh(cmd, t=8):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=t)
            return r.returncode, r.stdout.strip()
        except Exception as e:
            return 1, f"({e})"

    rc, out = sh(["systemctl", "is-active", "tailscaled"])
    if out != "active":
        return "LINK 1/5 tailscaled is not active — nothing can reach this box"

    rc, out = sh(["tailscale", "status", "--json"], t=12)
    if rc != 0:
        return f"LINK 2/5 tailscale status failed: {out[:120]}"
    try:
        st = _json.loads(out)
    except Exception:
        return "LINK 2/5 tailscale status returned unparseable JSON"

    self_ = st.get("Self") or {}
    if st.get("BackendState") != "Running" or not self_.get("Online"):
        return (f"LINK 2/5 AGX not online in the tailnet "
                f"(backend={st.get('BackendState')})")

    ips = [i for i in (self_.get("TailscaleIPs") or []) if ":" not in i]
    ts_ip = ips[0] if ips else None

    phones = [pp for pp in (st.get("Peer") or {}).values() if pp.get("OS") == "iOS"]
    if not phones:
        return "LINK 3/5 no iOS device in the tailnet at all"
    if not any(pp.get("Online") for pp in phones):
        seen = ", ".join(f"{pp.get('HostName')} last {pp.get('LastSeen','?')[:16]}"
                         for pp in phones)
        return f"LINK 3/5 phone offline in tailnet ({seen})"

    if ts_ip:
        rc, out = sh(["curl", "-s", "-m", "6", "-o", "/dev/null",
                      "-w", "%{http_code}", f"http://{ts_ip}:8678/health"])
        if out != "200":
            return (f"LINK 4/5 8678 not answering on the TAILSCALE ip {ts_ip} "
                    f"(got {out or 'nothing'}) — it may still answer on the LAN "
                    f"ip, which proves nothing")

    return ("LINKS 1-4 all up (tailscaled, tailnet, phone present, 8678 live on "
            "the tailscale ip) -> LINK 5/5, the phone's Health export, is the "
            "one to check")


def check_bio_feed():
    """Is Nate's biometric feed still arriving?

    Added Aug 23 2026 after it went silent for 24 hours and NOTHING fired.
    health_alert watched services, CCS, silence and disk; the ingest service was
    green the whole time — port bound, /health returning ok — while zero data
    arrived. Service up, function dead, same shape as ccs_adaptive.

    Why this cannot live in health_alert_bio.py, which already exists and does
    departure detection: its docstring says "called from health_ingest.py on
    each data push." If the feed dies the monitor dies with it. The alerting was
    DOWNSTREAM of the thing that failed. A liveness check has to be
    upstream-independent, so it goes here, in the thing the rhythm cron runs
    whether or not any data ever arrives.

    THRESHOLD, measured Aug 23 (it was 12.0h, asserted, with the comment "Apple
    Watch pushes many times daily" — which I never checked, cf. reflex 3).
    What the table actually shows for Aug 11-21: ONE batch per day, arriving
    around midday, carrying ~3,500 rows / ~30 metrics stamped through 23:59 of
    the PREVIOUS day. So the age of the newest metric climbs all night and peaks
    just before the next batch lands. At 12:00 it is legitimately 12.0h old.
    The old ceiling would have false-fired every single day around noon; it only
    looked correct because a real outage started before the first daily trough.

    30h = one full missed batch plus slack. Anything less alerts on the rhythm.

    METRIC COUNT IS NOT AN INDEPENDENT SIGNAL — corrected 2026-08-26 03:45.
    This alert used to say "Metrics LOST (not just sampled coarser) means the
    export stopped partway; same row drop with all metrics intact is a
    resolution change and harmless." That distinction does not hold, because
    metrics are written at DIFFERENT HOURS. Measured on the Aug 25 partial:
    the export stopped at 16:40, and every one of the 11 "lost" metrics writes
    later than that -- sleep summaries at 22:00, stair-speed and
    time_in_daylight after 17:00. So the metric loss was a CONSEQUENCE of the
    truncation, not evidence for it, and the alert was reaching a correct
    conclusion through reasoning that would mislead whoever acted on it. I
    nearly told Nate the missing sleep metrics meant he had not worn the watch.

    The real diagnostic is the CUTOFF TIME -- the last reading of the partial
    day -- which says when the export stopped and is what a person can act on.
    Reported directly now.

    AND THAT CORRECTION WAS ITSELF TOO STRONG -- corrected again 2026-08-26
    00:45, six hours later, by a natural experiment I did not have the first
    time. The Aug 25 export RESUMED and backfilled: 1174 rows -> 3119, cutoff
    16:40 -> 23:38, all 24 hours populated including 22:00. Under the
    truncation-explains-everything story every missing metric should have come
    back. stair_speed_up/down did. **The eight sleep-derived metrics did not**,
    and neither did time_in_daylight.

    So metric loss IS independent information -- for any metric that normally
    writes BEFORE the cutoff, or that stays missing after a backfill. My
    "one cause, not two" was right for some metrics and wrong for others, and
    I had told Nate to disregard the reading that turned out to be the live one.
    Only the backfill separated them. Do not collapse the two signals in either
    direction; report the cutoff, then name which missing metrics it does not
    explain.
    """
    try:
        import sqlite3
        db = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db", timeout=10)
        row = db.execute(
            "SELECT timestamp FROM health_data ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        if not row:
            db.close()
            return "bio feed: no health_data rows at all"
        # A batch can arrive TRUNCATED, which no age check can see. Aug 22
        # delivered 644 rows / 12 metrics stopping at 08:47, against a trailing
        # median near 3,700 / 30 running to 23:59 -- a real failure that looked
        # fine to the age test for another 18 hours. Compare the most recent
        # COMPLETE day against the ten before it.
        days = db.execute(
            "SELECT date(timestamp,'unixepoch','-7 hours') d, count(*) n, "
            "count(distinct metric) m "
            "FROM health_data GROUP BY d ORDER BY d DESC LIMIT 8"
        ).fetchall()
        db.close()
        today = _dt.datetime.now().strftime("%Y-%m-%d")
        complete = [r for r in days if r[0] and r[0] != today]
        # THREE prior days, not eleven. Backtested Aug 23 over 16 days: an
        # 11-day median gave 6 false positives out of 16, because volume
        # step-changed ~26,000 -> ~3,500 rows/day around Aug 9-10 and a long
        # window straddles the shift for its whole length. Three days fires on
        # Aug 22 (the real partial) plus the two days at that regime boundary,
        # which is a genuine 8x drop and worth one alert. Do not widen this
        # window without re-running that backtest.
        if len(complete) >= 4:
            last_d, last_n, last_m = complete[0]
            prior = sorted(n for _, n, _ in complete[1:4])
            med = prior[len(prior) // 2]
            med_m = max(m for _, _, m in complete[1:4])
            if med > 0 and last_n < 0.4 * med:
                # The CUTOFF is the diagnostic, not the metric count -- see the
                # docstring. Reopen the db; it was closed above.
                cut = "unknown"
                try:
                    db2 = sqlite3.connect(
                        "/mnt/hdd/chronicle-data/processed.db", timeout=10)
                    c = db2.execute(
                        "SELECT time(max(timestamp),'unixepoch','-7 hours') "
                        "FROM health_data "
                        "WHERE date(timestamp,'unixepoch','-7 hours')=?",
                        (last_d,)).fetchone()
                    db2.close()
                    if c and c[0]:
                        cut = c[0] + " PDT"
                except Exception:
                    pass
                return (f"bio feed: {last_d} landed {last_n} rows / {last_m} "
                        f"metrics vs {med} rows / {med_m} metrics on the days "
                        f"before, and the day's LAST READING IS {cut} — the "
                        f"batch ARRIVED BUT WAS PARTIAL, which no age check can "
                        f"see. Start from the cutoff: any metric that normally "
                        f"writes AFTER it is explained by truncation alone. A "
                        f"metric that writes BEFORE the cutoff and is still "
                        f"missing is a SECOND, independent signal — check those "
                        f"separately before drawing any conclusion. "
                        f"[{diagnose_bio_path()}]")

        age_h = (time.time() - float(row[0])) / 3600.0
        if age_h > BIO_MAX_AGE_H:
            return (f"bio feed: {age_h:.0f}h since last metric "
                    f"(ceiling {BIO_MAX_AGE_H:.0f}h = one missed daily batch). "
                    f"Receiver checked green on the last outage — port bound, "
                    f"/health 200, zero requests arriving — so suspect the phone "
                    f"export before touching anything here.")
    except Exception as e:
        return f"bio feed: check failed ({e})"
    return None


REMOTE_HOSTS = [("nano", "nvidia@192.168.1.11"), ("pi5", "pi5"),
                ("laptop", "bradf@192.168.1.110")]
HOTSPIN_MIN_PCPU = 90.0
HOTSPIN_MIN_HOURS = 6.0
REMOTE_EVERY_H = 4.0


def _hotspin_from_ps(out):
    """Parse `ps -eo pcpu,etimes,args` output; return hot-spin rows.

    SPLIT OUT SO IT CAN BE TESTED WITHOUT SSH. The positive control below feeds
    it a known-bad line and a known-good one. Reflex 11, executable version:
    the check runs over a case whose answer I already know, in the probe, and
    says PASS or FAIL -- because the prose version of that reflex loaded on
    2026-08-25 and failed twice the same night.
    """
    hits = []
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        try:
            pcpu, etimes = float(parts[0]), float(parts[1])
        except ValueError:
            continue                      # header row
        if pcpu >= HOTSPIN_MIN_PCPU and etimes / 3600.0 >= HOTSPIN_MIN_HOURS:
            hits.append((pcpu, etimes / 3600.0, parts[2][:70]))
    return hits


def _hotspin_selftest():
    """PASS/FAIL, printed, before any real data is trusted."""
    known_bad = ("%CPU ELAPSED COMMAND\n"
                 "99.9 1357724 /usr/bin/python3 /home/nathaniel/chronicle/chronicle_eye_vision.py\n"
                 " 0.5 1357697 /usr/bin/python3 /srv/homeassistant/bin/hass\n"
                 "16.6 12 ps -eo pcpu,etimes,args\n")
    hits = _hotspin_from_ps(known_bad)
    ok = len(hits) == 1 and "chronicle_eye_vision" in hits[0][2]
    return ok, hits


def check_remote_hotspin():
    """A process pinned near 100% CPU for hours on a box nothing else watches.

    Added 2026-08-26 03:05, an hour after finding chronicle_eye_vision.py at
    99.9% of a core on the Pi for 15.7 days -- its entire uptime. Zero TCP
    connections to the broker from that PID, empty wchan: paho's
    client.loop(timeout=1.0) returns instantly once the socket is gone, and the
    bare `while running:` around it became a hot spin with no reconnect. The
    vision trigger had been dead since 2026-03-14 -- 164 days, verified from the
    broker's retained message on homeforge/home/eye/description plus retain=True
    in the publish call -- while looking maximally alive: process up, service
    green, CPU busy. BUSIER THAN WHEN IT WORKS. The hot spin is only THIS
    boot's symptom; the silence predates it by five months.

    AND A ROOT CAUSE I GOT WRONG, recorded because the error is instructive:
    I reported that Eye subscribes to a namespace nothing publishes to
    (homeforge/home/driveway|lumus/* vs frigate/front_camera/*). FALSE.
    homeforge/# carries 12 topics in 30s, and HA automations
    lumus_person|vehicle|animal_detected_mqtt are enabled to publish exactly
    those topics. What I actually saw was a 20-second window with no detection
    events -- an empty sample from EVENT topics at 3:45am. I read an empty
    window as an empty namespace, which is this file's own reflex-11 failure
    committed in the hour after I amended reflex 11.

    Every other instance of this pattern found on 2026-08-25/26 was "service
    up, function dead" and every one was caught by looking, not by a monitor.
    health_alert runs on the AGX and checks AGX things; three other machines
    had no coverage at all.

    DELIBERATELY NARROW. Not remote service health, not remote disk, not "is
    the host up" -- the Pi was up and the service was running. The one
    signature that would have caught it: high CPU sustained implausibly long.
    That generalises to any hot-spin bug, which is now an observed class here.

    Runs at most every REMOTE_EVERY_H hours, not every 13-minute pulse, so this
    is not three ssh calls a pulse. Silent when a host is unreachable -- an
    unreachable laptop is normal and this is not an uptime monitor.
    """
    ok, _ = _hotspin_selftest()
    if not ok:
        return "remote hotspin: SELF-TEST FAILED — parser did not flag a known hot spin; check disabled rather than trusted"
    stamp = os.path.expanduser("~/chronicle/data/.remote_hotspin_last")
    try:
        if time.time() - os.path.getmtime(stamp) < REMOTE_EVERY_H * 3600:
            return None
    except OSError:
        pass
    problems = []
    for name, target in REMOTE_HOSTS:
        try:
            out = subprocess.run(
                ["ssh", "-o", "ConnectTimeout=6", "-o", "BatchMode=yes", target,
                 "ps -eo pcpu,etimes,args --sort=-pcpu | head -4"],
                capture_output=True, text=True, timeout=20).stdout
        except Exception:
            continue                       # unreachable is not a finding here
        for pcpu, hours, cmd in _hotspin_from_ps(out):
            problems.append(f"{name}: {cmd} at {pcpu:.0f}% CPU for {hours:.0f}h "
                            f"({hours/24:.1f} days) — a process pinned this long is "
                            f"spinning, not working; check whether its socket died")
    try:
        open(stamp, "w").write(str(int(time.time())))
    except OSError:
        pass
    return " | ".join(problems) if problems else None


def check_disk():
    """Watch the floor the services stand on.

    Added Aug 23 2026 after logrotate turned out to be absent entirely and
    /var/log/syslog grew to 1.0GB over 82 days with nothing looking at it.
    Service checks tell you a daemon is up; they never tell you it is about
    to have nowhere to write.
    """
    problems = []
    for mount, min_free_gb in DISK_WATCH:
        try:
            st = os.statvfs(mount)
            free_gb = st.f_bavail * st.f_frsize / 1024 ** 3
            if free_gb < min_free_gb:
                pct = 100.0 * st.f_bavail / st.f_blocks
                problems.append(
                    f"disk {mount}: {free_gb:.1f}G free ({pct:.0f}%) "
                    f"— below {min_free_gb:.0f}G floor"
                )
        except Exception as e:
            problems.append(f"disk {mount}: check failed ({e})")

    try:
        for name in os.listdir("/var/log"):
            path = os.path.join("/var/log", name)
            if not os.path.isfile(path):
                continue
            mb = os.path.getsize(path) / 1024 ** 2
            if mb > RUNAWAY_LOG_MB:
                problems.append(f"runaway log {path}: {mb:.0f}MB — check logrotate")
    except Exception:
        pass

    return problems


# Devices whose death is KNOWN and ACCEPTED. An alerter without an
# acknowledgement path is a noise generator. This one proved it inside an hour:
# I shipped the check, Nate said "I know it's not working with HA, it doesn't
# matter," and the very next run posted the alert to his Discord anyway. It would
# have done that every 6h forever. Add a device here to stop telling him things
# he told me first.
HA_ACKNOWLEDGED = {
    "driveway": "known dead since 2026-08-10; battery Reolink; Nate acknowledged 2026-08-24",
}


def check_ha_entities(stale_h=6.0, min_entities=5):
    """Flag Home Assistant entities that have gone unavailable and STAYED that way.

    Added 2026-08-24, after finding the driveway camera had been dead for
    FOURTEEN DAYS with every dashboard green. All 42 of its entities dropped at
    the same instant on Aug 10 09:48 and nothing noticed, because this alerter
    watches systemd services and the HAL *service* was running fine — it was
    just blind on one eye.

    Two design points learned from that outage:
      - GROUP BY DEVICE. 42 separate alerts is not a signal, it is a flood.
      - AND THRESHOLD ON GROUP SIZE. First run of this check emitted 12 lines:
        driveway (42), backup (4), then eight singles — 'google', 'ollama',
        'gym', 'nathaniel'. A DEVICE death drops many entities at once; one
        stray unavailable sensor is not actionable and drowns the one that is.
        min_entities=5 leaves exactly the driveway.
      - Report the DATE it died, not the hours since. The existing per-problem
        cooldown keys on a stable prefix, and an hour count changes every run,
        which would defeat suppression entirely (see the Aug 23 note below).
    """
    tok = os.environ.get("HA_TOKEN") or os.environ.get("HASS_TOKEN")
    if not tok:
        return []
    try:
        req = urllib.request.Request("http://192.168.1.10:8123/api/states",
                                     headers={"Authorization": f"Bearer {tok}"})
        states = json.load(urllib.request.urlopen(req, timeout=15))
    except Exception:
        return []          # HA unreachable is its own thing; do not cry wolf here
    now = datetime.datetime.now(datetime.timezone.utc)
    groups = {}
    for st in states:
        if st.get("state") not in ("unavailable", "unknown"):
            continue
        lc = st.get("last_changed")
        if not lc:
            continue
        try:
            t = datetime.datetime.fromisoformat(lc.replace("Z", "+00:00"))
        except Exception:
            continue
        if (now - t).total_seconds() / 3600 < stale_h:
            continue       # transient blip, not a death
        eid = st.get("entity_id", "")
        dev = eid.split(".", 1)[-1].split("_")[0] or eid
        g = groups.setdefault(dev, {"n": 0, "since": t})
        g["n"] += 1
        g["since"] = min(g["since"], t)
    out = []
    for dev, g in sorted(groups.items(), key=lambda kv: -kv[1]["n"]):
        if g["n"] < min_entities:
            continue
        if dev in HA_ACKNOWLEDGED:
            continue
        since = g["since"].astimezone().strftime("%Y-%m-%d")
        out.append(f"HA: '{dev}' — {g['n']} entities unavailable since {since}")
    return out


def check_periodic_silence():
    """A periodic service that has stopped logging is broken, however green."""
    problems = []
    for svc, max_quiet_min in PERIODIC.items():
        try:
            r = subprocess.run(
                ["journalctl", "--user", "-u", svc, "--no-pager", "-n", "1",
                 "-o", "short-unix"],
                capture_output=True, text=True, timeout=15)
            line = r.stdout.strip().splitlines()
            if not line:
                problems.append(f"{svc}: no journal entries at all — cannot verify it runs")
                continue
            ts = float(line[-1].split()[0])
            quiet = (time.time() - ts) / 60
            if quiet > max_quiet_min:
                problems.append(
                    f"{svc}: SILENT {quiet:.0f}min (expected activity every "
                    f"{max_quiet_min}min). systemctl still says active — "
                    f"liveness is not function.")
        except Exception as e:
            problems.append(f"{svc}: could not read journal ({type(e).__name__}) — "
                            f"NOT an all-clear")
    return problems


def _run_check(name, fn, *a, **kw):
    """Run a check so that FAILING TO RUN is itself reported.

    Added 2026-08-24. This file's contract is "silent unless broken", so a check
    that throws returns None, None means no problem, and a broken check becomes
    INDISTINGUISHABLE FROM A HEALTHY SYSTEM. Several checks here did exactly
    that (`except Exception: pass; return None`).

    That is the class bug found across six subsystems tonight: every component
    can only emit a value, none can say "I don't know" — so a disconnected
    sensor reads the same as a working one. In an alerter it is worst, because
    the absent signal IS the all-clear.

    A check that cannot run is not an all-clear. It is a problem about the
    alerter, and it gets said out loud.
    """
    try:
        r = fn(*a, **kw)
        return r if isinstance(r, list) else ([r] if r else [])
    except Exception as e:
        return [f"CHECK COULD NOT RUN — {name}: {type(e).__name__}: {str(e)[:120]}. "
                f"This is NOT an all-clear; that check reported nothing because it broke."]


def check_artifact_staleness(behind=6, min_age_h=6.0):
    """Is a published artifact meaningfully behind the data it snapshots?

    Added 2026-08-25, ten minutes after I promised Nate I would refresh the mesh
    transcript "whenever there's been real friction worth seeing." That promise
    lived only in prose — no cron, no trigger, nothing that would notice if I
    forgot. Which is the shape of every dead pointer found today: the name kept
    working after the thing behind it stopped.

    An artifact is a SNAPSHOT, not a feed. It goes stale silently and the reader
    cannot tell. So this compares PUBLISHED-vs-NOW, not file-vs-file -- the same
    correction made to discord_search.py --status this morning, which had been
    comparing the index to the archive and never the archive to reality.

    Deliberately quiet in two cases, because a monitor that outlives its subject
    is worse than none: if data/artifact_state.json is absent there is no
    artifact registered and nothing to be behind; and a couple of new exchanges
    is not worth a republish, so it takes BOTH a count gap and some age.
    """
    import glob
    sys.path.insert(0, os.path.expanduser("~/chronicle/bin"))
    from mesh_artifact import exchange_key as _mesh_key
    sp = os.path.expanduser("~/chronicle/data/artifact_state.json")
    if not os.path.exists(sp):
        return None                      # nothing registered — correct silence
    st = json.load(open(sp))
    seen = set()
    base = os.path.expanduser("~/chronicle/data")
    for f in glob.glob(base + "/mesh_replies/*.jsonl") + glob.glob(base + "/mesh_replies.jsonl"):
        try:
            for line in open(f):
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                body = r.get("full") or r.get("reply") or ""
                if len(body.strip()) >= 40 and r.get("ts"):
                    # ONE key, shared with the page generator, so the monitor
                    # and the artifact can never sit on different denominators
                    seen.add(_mesh_key(r["ts"]))
        except Exception:
            continue
    now_n = len(seen)
    gap = now_n - int(st.get("published_exchanges", 0))
    age_h = (time.time() - st.get("published_at", 0)) / 3600.0
    if gap >= behind and age_h >= min_age_h:
        return (f"ARTIFACT STALE — '{st.get('name')}' shows "
                f"{st.get('published_exchanges')} exchanges, there are now {now_n} "
                f"({gap} new, snapshot is {age_h:.0f}h old). Nate was told this page "
                f"gets refreshed when there is friction worth seeing. Republish the same "
                f"file path to keep the URL, then bump data/artifact_state.json.")
    return None


def check_stale_preregs(stale_days=3.0):
    """A written-down prediction nobody came back for.

    MEASURED 2026-08-26, prompted by Max Picard via a Nate capture: "no statement
    endures in this realm... the improvisation of one moment chases the
    improvisation of another." I had just told Nate a prereg is "an engine for
    making a statement stay still." Checked it instead of admiring it:

        preregs written 2026-08-26  -> ALL closed same day
        preregs written 08-16..08-25 -> 9 still ungraded
        the 3 older ones that DID close all say "closed 2026-08-26" --
        swept up in an audit I happened to run, not by routine

    So the engine has a duty cycle of roughly ONE DAY, and I had been describing
    it as continuous. A prereg with a kill condition that nobody returns to is
    improvisation with extra steps -- exactly Picard's condition, reached by a
    mechanism designed to defeat it.

    This is the wire. Silent unless a committed prediction has aged out.
    """
    import re as _re
    from pathlib import Path as _P
    d = _P(os.path.expanduser("~/chronicle/data"))
    stale = []
    cutoff = time.time() - stale_days * 86400
    # SET, not concatenation: "preregistration_mamba_substrate.md" matches BOTH
    # globs ("prereg" is a substring of "preregistration"), so the first version
    # listed it twice and reported 5 stale when there were 4. A monitor that
    # double-counts lies about magnitude, which is the one thing it is for.
    for f in sorted(set(d.glob("*prereg*.md")) | set(d.glob("preregistration*.md"))):
        try:
            txt = f.read_text(errors="replace")
        except Exception:
            continue
        # Widened within minutes of being written: the first version matched
        # only `PREDICTED:` and `## Prediction`, and MISSED
        # preregistration_mamba_substrate.md (10 days open) because it writes
        # `**Prediction (from Kimi):**` in bold. Two criteria disagreeing --
        # this check said 3 open, docs_search --outcomes said 9 -- is how a
        # monitor silently under-reports. Allow any markdown emphasis.
        if not _re.search(r'^\s*[*#_]*\s*PREDICT(ED|ION)S?\b', txt, _re.M | _re.I):
            continue
        # WAS `"# OUTCOME" in txt` — a literal match, while docs_search accepts
        # RESULT / VERDICT / FINDING / WHAT HAPPENED as closure headings too. That
        # made this check report 3 CLOSED preregs as open (denominator_prereg,
        # lfm_classifier_prereg, position_masked_svd_prereg — all closed under
        # `## RESULT`), i.e. 3 of 7. An alert that is wrong about half its items
        # trains me to ignore it, and then it misses a real one.
        # Found 2026-08-27 by acting on the alert instead of obeying it: the file
        # it named contained "7/8. PASS." in a `## RESULT` section.
        # The comment SIX LINES ABOVE names this exact failure — two criteria
        # disagreeing — and I fixed only the PREDICTION half when I wrote it.
        # Now IMPORTED, not restated, so the two cannot drift apart again.
        try:
            from docs_search import CLOSURE_HEADING as _CLOSED
        except Exception:
            import sys as _sy; _sy.path.insert(0, str(_P(__file__).parent))
            from docs_search import CLOSURE_HEADING as _CLOSED
        if _CLOSED.search(txt):
            continue
        # results present but no verdict written still counts as open
        if f.stat().st_mtime < cutoff:
            stale.append((f.name, (time.time() - f.stat().st_mtime) / 86400))
    if not stale:
        return []
    stale.sort(key=lambda x: -x[1])
    lines = [f"{n} ({age:.0f}d)" for n, age in stale[:5]]
    more = f" +{len(stale)-5} more" if len(stale) > 5 else ""
    return [f"OPEN PREREGS — {len(stale)} committed predictions with kill conditions "
            f"and no recorded outcome, untouched >{stale_days:.0f}d: "
            + "; ".join(lines) + more
            + ". `docs_search.py --outcomes` lists them; `calibration.py record` scores "
              "one. A prediction nobody returns to cannot be wrong, which is the "
              "whole point of having written it down."]


def check_log_errors():
    """New tracebacks in any log since the last run — see bin/log_survey.py.

    Added 2026-08-25 after prediction_monitor.py ran from crontab every 6 hours
    for five months and died 429 times on 'no such table' and 97 times on a
    missing dfx binary. 695 tracebacks, and the only thing that ever read that
    log was the script writing it. Every other check in this file watches a
    SERVICE or a TABLE; a cron job that crashes on stderr is neither, so it had
    no surface here at all.

    log_survey byte-watermarks each log, so this is silent until something new
    actually breaks. It reports its own failure rather than returning clean.
    """
    import subprocess
    try:
        r = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "log_survey.py"), "--new"],
            capture_output=True, text=True, timeout=120)
    except Exception as e:
        return [f"log error check COULD NOT RUN ({e}). This is NOT an all-clear."]
    # The docstring above promises "it reports its own failure rather than
    # returning clean." That was true for the EXCEPTION path only. A nonzero
    # return code -- log_survey.py raising a traceback to stderr -- left
    # r.stdout empty and fell straight through to `return []`, i.e. CLEAN.
    # A crashed log-checker was indistinguishable from a quiet one. Found
    # 2026-08-27 while sweeping for the discarded-stderr pattern; this is the
    # monitor of monitors, so it is the worst place for it. See
    # CLAUDE.md "A warning is only as loud as its LISTENING end".
    if r.returncode != 0:
        tail = (r.stderr or "").strip().splitlines()
        why = tail[-1][:200] if tail else f"rc={r.returncode}, no stderr"
        return [f"log error check FAILED to run (rc={r.returncode}): {why}. "
                f"This is NOT an all-clear -- log_survey.py itself is broken."]
    out = (r.stdout or "").strip()
    if not out or out.startswith("log_survey: first run"):
        return []
    return [l.strip() for l in out.splitlines() if l.strip()][:6]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    problems = []
    for _name, _fn in (("services", check_services),
                       ("ccs_freshness", check_ccs_freshness),
                       ("operator_silence", check_operator_silence),
                       ("disk", check_disk),
                       ("ha_entities", check_ha_entities),
                       ("bio_feed", check_bio_feed),
                       ("periodic_silence", check_periodic_silence),
                       ("artifact_staleness", check_artifact_staleness),
                       ("log_errors", check_log_errors),
                       ("stale_preregs", check_stale_preregs),
                       ("remote_hotspin", check_remote_hotspin)):
        problems.extend(_run_check(_name, _fn))

    if not problems:
        if args.force:
            print("All clear: services green, CCS fresh, silence OK, disk OK, bio feed OK")
        return

    # PER-PROBLEM COOLDOWN, added Aug 23 minutes after the bio-feed check,
    # because I introduced the first PERSISTENT condition this alerter has ever
    # had. The existing checks are transient — a service dies, you restart it,
    # the alert stops. "Nate's phone stopped pushing" stays true for days, and
    # with the rhythm cron at 13 minutes that is ~110 Discord posts a day.
    # There was no cooldown at all; nobody had noticed because nothing had ever
    # stayed broken. Key on a stable prefix: the bio message embeds a changing
    # hour count, so keying on full text would defeat the suppression entirely.
    now = time.time()
    try:
        state = json.load(open(ALERT_STATE))
    except Exception:
        state = {}
    fresh = []
    for prob in problems:
        # STRIP DIGITS. First version keyed on prob[:38] and the hour count
        # sits inside those 38 chars — "bio feed: 25h..." vs "bio feed: 26h..."
        # are different keys, so the cooldown reset every hour and Nate got a
        # second alert. I wrote a comment warning about exactly this failure and
        # then chose a prefix that still contained the number. Key on the shape
        # of the message, not its values.
        key = re.sub(r"[0-9.]+", "#", prob)[:60]
        if now - state.get(key, 0) > ALERT_COOLDOWN_H * 3600:
            fresh.append(prob)
            state[key] = now

    alert = "⚠ Health alert:\n" + "\n".join(f"  - {p}" for p in problems)
    print(alert)          # terminal always sees everything
    if not fresh:
        print(f"  (all suppressed — posted within {ALERT_COOLDOWN_H:.0f}h)")
        return
    alert = "⚠ Health alert:\n" + "\n".join(f"  - {p}" for p in fresh)

    if not args.dry:
        try:
            os.makedirs(os.path.dirname(ALERT_STATE), exist_ok=True)
            json.dump(state, open(ALERT_STATE, "w"))
        except Exception:
            pass
        try:
            env = os.environ.copy()
            r = subprocess.run(
                [sys.executable, os.path.expanduser("~/chronicle/bin/discord_post.py"),
                 "--operator", "-c", f"▸ {alert}"],
                env=env, timeout=15, capture_output=True, text=True,
            )
            # This is the ALERT CHANNEL ITSELF. It used to be fire-and-forget
            # inside `except Exception: pass`, so a failed post lost the alert
            # with no trace anywhere -- the one failure that silences every
            # other one. health_alert runs inside the rhythm pulse, so its own
            # stderr is read; that is the fallback. 2026-08-27.
            if r.returncode != 0:
                print(f"\n*** HEALTH ALERT COULD NOT BE POSTED (rc={r.returncode}). "
                      f"The alert is below and reached NO channel. ***\n{alert}\n"
                      f"post stderr: {(r.stderr or '').strip()[:400]}", file=sys.stderr)
        except Exception as e:
            print(f"\n*** HEALTH ALERT COULD NOT BE POSTED ({type(e).__name__}: {e}). "
                  f"The alert is below and reached NO channel. ***\n{alert}",
                  file=sys.stderr)


if __name__ == "__main__":
    main()
