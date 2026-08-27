#!/usr/bin/env python3
"""tweet_fetch — fetch tweet(s) by ID with full text + images. Wraps xmcp_call.py.

Usage:
  tweet_fetch.py 2057931456870236562
  tweet_fetch.py 2057931456870236562 2057929293926174852
  tweet_fetch.py --search "from:burny_tech recombination"
  tweet_fetch.py --thread 2086905991388360818    # fetch full thread from any tweet in it

Handles: singular 'id' param, note_tweet for long posts, author expansion,
         media/image download, quote tweet resolution, thread reconstruction.
Never guess xmcp parameters again.
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

XMCP = Path.home() / "chronicle" / "bin" / "xmcp_call.py"
ENV_FILE = Path.home() / "chronicle" / "chronicle.env"
IMG_DIR = Path("/tmp/tweet_images")


def _load_env() -> dict:
    env = {}
    if ENV_FILE.is_file():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip("'\"")
    return env


def _call_xmcp(tool: str, params: dict) -> dict:
    result = subprocess.run(
        [sys.executable, str(XMCP), tool, json.dumps(params)],
        capture_output=True, text=True, timeout=30,
    )
    output = result.stdout.strip()
    lines = [l for l in output.splitlines() if not l.strip().startswith(("/", "warn"))]
    cleaned = "\n".join(lines)
    brace = cleaned.find("{")
    if brace >= 0:
        try:
            return json.loads(cleaned[brace:])
        except json.JSONDecodeError:
            pass
    if result.returncode != 0:
        err = result.stderr.strip().split("\n")[-1] if result.stderr else "unknown error"
        return {"error": err}
    return {"error": "no JSON in output", "raw": output[:500]}


def _download_image(url: str, tweet_id: str, idx: int = 0) -> str:
    IMG_DIR.mkdir(exist_ok=True)
    ext = ".jpg"
    if ".png" in url:
        ext = ".png"
    local = IMG_DIR / f"tweet_{tweet_id}_{idx}{ext}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "chronicle-opus/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            local.write_bytes(resp.read())
        if local.stat().st_size > 100:
            return str(local)
    except Exception:
        pass
    return ""


def _extract_media_urls(data: dict) -> list[str]:
    """Extract image URLs from an already-fetched API response."""
    image_urls = []
    for m in data.get("includes", {}).get("media", []):
        if m.get("type") == "video":
            u = m.get("preview_image_url", "")
        else:
            u = m.get("url") or m.get("preview_image_url", "")
        if u:
            image_urls.append(u)
        if len(image_urls) >= 10:
            break
    return image_urls


def _fetch_media_bearer(tweet_id: str, _depth: int = 0) -> list[str]:
    """Fallback: use bearer token to get media URLs when xmcp doesn't return them."""
    if _depth > 1:
        return []
    env = _load_env()
    token = env.get("X_BEARER_TOKEN", "")
    if not token:
        return []
    url = (f"https://api.x.com/2/tweets/{tweet_id}"
           f"?expansions=attachments.media_keys"
           f"&media.fields=url,type,preview_image_url"
           f"&tweet.fields=attachments,entities")
    try:
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "chronicle-opus/1.0",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        image_urls = _extract_media_urls(data)

        if _depth == 0:
            entities = data.get("data", {}).get("entities", {})
            for eu in entities.get("urls", []):
                expanded = eu.get("expanded_url", "")
                if "/status/" in expanded and expanded != f"https://twitter.com/i/status/{tweet_id}":
                    import re
                    qt_match = re.search(r'/status/(\d+)', expanded)
                    if qt_match:
                        qt_id = qt_match.group(1)
                        qt_urls = _fetch_media_bearer(qt_id, _depth=_depth + 1)
                        image_urls.extend(qt_urls)

        return image_urls
    except Exception:
        return []


def _fetch_bearer(tweet_id: str) -> dict | None:
    """Fetch tweet via bearer token — gets note_tweet that xmcp often drops."""
    import time as _time
    env = _load_env()
    token = env.get("X_BEARER_TOKEN", "")
    if not token:
        return None
    url = (f"https://api.x.com/2/tweets/{tweet_id}"
           f"?tweet.fields=text,note_tweet,author_id,entities,attachments"
           f"&expansions=author_id,attachments.media_keys"
           f"&media.fields=url,type,preview_image_url"
           f"&user.fields=username,name")
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": "chronicle-opus/1.0",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                _time.sleep(2 ** attempt)
                continue
            print(f"[tweet_fetch] bearer fallback failed: HTTP {e.code}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"[tweet_fetch] bearer fallback failed: {e}", file=sys.stderr)
            return None
    return None


def fetch_tweet(tweet_id: str) -> dict:
    data = _call_xmcp("getPostsById", {
        "id": tweet_id,
        "expansions": "author_id,attachments.media_keys",
        "tweet.fields": "text,author_id,note_tweet,entities,attachments",
        "media.fields": "url,type,preview_image_url",
    })

    if "error" in data:
        return {"id": tweet_id, "error": data["error"]}

    tweet = data.get("data", {})
    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
    author = users.get(tweet.get("author_id"), {})

    note = tweet.get("note_tweet", {})
    full_text = note.get("text", "") if note else tweet.get("text", "")

    # xmcp often drops note_tweet — if text looks truncated, try bearer token
    raw_text = tweet.get("text", "")
    truncated = False
    bearer_data = None
    _ends_clean = raw_text.rstrip().endswith(('.', '!', '?', '"', "'", ')', ']', '…', '️'))
    if not note and (len(raw_text) >= 250 or (len(raw_text) >= 140 and not _ends_clean)):
        bearer_data = _fetch_bearer(tweet_id)
        if bearer_data:
            b_tweet = bearer_data.get("data", {})
            b_note = b_tweet.get("note_tweet", {})
            if b_note and b_note.get("text"):
                full_text = b_note["text"]
            else:
                truncated = True
            if not users:
                users = {u["id"]: u for u in bearer_data.get("includes", {}).get("users", [])}
                author = users.get(tweet.get("author_id"), {})
        else:
            truncated = True

    # Get images — prefer xmcp includes, then reuse bearer data, then dedicated bearer call
    image_urls = _extract_media_urls(data)
    if not image_urls and bearer_data:
        image_urls = _extract_media_urls(bearer_data)
    if not image_urls and not bearer_data:
        image_urls = _fetch_media_bearer(tweet_id)

    # Download images (cap at 10 to prevent /tmp exhaustion)
    MAX_IMAGES = 10
    image_paths = []
    for i, url in enumerate(image_urls[:MAX_IMAGES]):
        path = _download_image(url, tweet_id, i)
        if path:
            image_paths.append(path)
    if len(image_urls) > MAX_IMAGES:
        image_paths.append(f"[{len(image_urls) - MAX_IMAGES} more images omitted]")

    # Resolve quote tweet text — single xmcp call, bearer fallback for long posts
    quoted_text = ""
    quoted_author = ""
    entities = tweet.get("entities", {})
    if bearer_data:
        b_entities = bearer_data.get("data", {}).get("entities", {})
        if b_entities:
            entities = b_entities
    for eu in entities.get("urls", []):
        expanded = eu.get("expanded_url", "")
        if "/status/" in expanded:
            import re
            qt_match = re.search(r'/status/(\d+)', expanded)
            if qt_match:
                qt_id = qt_match.group(1)
                if qt_id != tweet_id:
                    qt_data = _call_xmcp("getPostsById", {
                        "id": qt_id,
                        "expansions": "author_id,attachments.media_keys",
                        "tweet.fields": "text,author_id,note_tweet,attachments",
                        "media.fields": "url,type,preview_image_url",
                    })
                    qt_tweet = qt_data.get("data", {})
                    qt_users = {u["id"]: u for u in qt_data.get("includes", {}).get("users", [])}
                    qt_author_data = qt_users.get(qt_tweet.get("author_id"), {})
                    qt_note = qt_tweet.get("note_tweet", {})
                    quoted_text = qt_note.get("text", "") if qt_note else qt_tweet.get("text", "")
                    quoted_author = qt_author_data.get("username", "")
                    # Bearer fallback for truncated quote tweets
                    qt_raw = qt_tweet.get("text", "")
                    if not qt_note and len(qt_raw) >= 270:
                        qt_bearer = _fetch_bearer(qt_id)
                        if qt_bearer:
                            qt_b = qt_bearer.get("data", {})
                            qt_b_note = qt_b.get("note_tweet", {})
                            if qt_b_note and qt_b_note.get("text"):
                                quoted_text = qt_b_note["text"]
                            if not image_paths:
                                for url in _extract_media_urls(qt_bearer):
                                    path = _download_image(url, qt_id, len(image_paths))
                                    if path:
                                        image_paths.append(path)
                    elif not image_paths:
                        for m in qt_data.get("includes", {}).get("media", []):
                            u = m.get("url") or m.get("preview_image_url", "")
                            if u:
                                path = _download_image(u, qt_id, len(image_paths))
                                if path:
                                    image_paths.append(path)

    result = {
        "id": tweet.get("id", tweet_id),
        "author": author.get("username", "unknown"),
        "name": author.get("name", ""),
        "text": full_text,
    }
    if truncated:
        result["truncated"] = True
        print(f"[tweet_fetch] WARNING: tweet {tweet_id} text appears truncated (bearer fallback failed)", file=sys.stderr)
    if image_paths:
        result["images"] = image_paths
    if quoted_text:
        result["quoted"] = {"author": quoted_author, "text": quoted_text}

    return result


def fetch_thread(tweet_id: str) -> list[dict]:
    """Fetch all tweets in a thread given any tweet ID from the thread.

    Handles both reply-chain threads (via conversation_id) and numbered
    threads (1/7, 2/7... pattern) which are common standalone posts.
    """
    import re

    data = _call_xmcp("getPostsById", {
        "id": tweet_id,
        "expansions": "author_id",
        "tweet.fields": "text,author_id,conversation_id,note_tweet",
    })

    tweet = data.get("data", {})
    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
    author = users.get(tweet.get("author_id"), {})
    username = author.get("username", "")
    note = tweet.get("note_tweet", {})
    text = note.get("text", "") if note else tweet.get("text", "")

    if not username:
        return [fetch_tweet(tweet_id)]

    # Try conversation_id approach first
    conv_id = tweet.get("conversation_id", "")
    if conv_id:
        search_data = _call_xmcp("searchPostsRecent", {
            "query": f"conversation_id:{conv_id} from:{username}",
            "max_results": 100,
            "tweet.fields": "text,author_id,note_tweet",
            "expansions": "author_id",
        })
        conv_tweets = search_data.get("data", [])
        if len(conv_tweets) > 1:
            conv_tweets.sort(key=lambda t: int(t.get("id", "0")))
            s_users = {u["id"]: u for u in search_data.get("includes", {}).get("users", [])}
            return [_tweet_to_result(t, s_users, username) for t in conv_tweets]

    # Detect numbered thread pattern (e.g., "3/7", "1/12")
    m = re.search(r'(\d+)/(\d+)', text)
    if not m:
        return [fetch_tweet(tweet_id)]

    total = int(m.group(2))
    if total < 2 or total > 30:
        return [fetch_tweet(tweet_id)]

    # Search for each numbered part
    found = {}
    for part in range(1, total + 1):
        search_data = _call_xmcp("searchPostsRecent", {
            "query": f'from:{username} "{part}/{total}"',
            "max_results": 10,
            "tweet.fields": "text,author_id,note_tweet",
            "expansions": "author_id",
        })
        for t in search_data.get("data", []):
            tid = t.get("id", "")
            t_note = t.get("note_tweet", {})
            t_text = t_note.get("text", "") if t_note else t.get("text", "")
            if re.search(rf'\b{part}/{total}\b', t_text) and tid not in found:
                s_users = {u["id"]: u for u in search_data.get("includes", {}).get("users", [])}
                found[tid] = _tweet_to_result(t, s_users, username)
                break

    if not found:
        return [fetch_tweet(tweet_id)]

    return [v for _, v in sorted(found.items(), key=lambda x: int(x[0]))]


def _tweet_to_result(tweet: dict, users: dict, fallback_author: str) -> dict:
    author = users.get(tweet.get("author_id"), {})
    note = tweet.get("note_tweet", {})
    full_text = note.get("text", "") if note else tweet.get("text", "")
    return {
        "id": tweet.get("id"),
        "author": author.get("username", fallback_author),
        "name": author.get("name", ""),
        "text": full_text,
    }


def search_tweets(query: str, max_results: int = 10) -> list[dict]:
    max_results = max(10, min(100, max_results))
    data = _call_xmcp("searchPostsRecent", {
        "query": query,
        "max_results": max_results,
        "tweet.fields": "text,author_id,note_tweet",
        "expansions": "author_id",
    })

    if "error" in data:
        return [{"error": data["error"]}]

    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
    results = []
    for tweet in data.get("data", []):
        author = users.get(tweet.get("author_id"), {})
        note = tweet.get("note_tweet", {})
        full_text = note.get("text", "") if note else tweet.get("text", "")
        results.append({
            "id": tweet.get("id"),
            "author": author.get("username", "unknown"),
            "text": full_text,
        })
    return results


def _cli() -> int:
    p = argparse.ArgumentParser(description="Fetch tweets by ID or search")
    p.add_argument("ids", nargs="*", help="Tweet IDs to fetch")
    p.add_argument("--search", "-s", help="Search query instead of IDs")
    p.add_argument("--thread", "-t", help="Fetch full thread from any tweet ID in it")
    p.add_argument("--compact", action="store_true", help="One line per tweet")
    args = p.parse_args()

    if args.thread:
        results = fetch_thread(args.thread)
        if args.compact:
            for i, r in enumerate(results, 1):
                print(f'{i}/{len(results)} @{r.get("author","?")}: {r.get("text","")[:200]}')
        else:
            json.dump(results, sys.stdout, indent=2)
            print()
    elif args.search:
        results = search_tweets(args.search)
        if args.compact:
            for r in results:
                print(f'@{r.get("author","?")}: {r.get("text","")[:200]}')
        else:
            json.dump(results, sys.stdout, indent=2)
            print()
    elif args.ids:
        results = [fetch_tweet(tid) for tid in args.ids]
        if args.compact:
            for r in results:
                imgs = r.get("images", [])
                img_note = f" [{len(imgs)} img]" if imgs else ""
                qt = r.get("quoted", {})
                qt_note = f" [QT @{qt['author']}]" if qt else ""
                print(f'@{r.get("author","?")}: {r.get("text","")[:180]}{img_note}{qt_note}')
                for img in imgs:
                    print(f'  -> {img}')
        else:
            json.dump(results, sys.stdout, indent=2)
            print()
    else:
        p.print_help()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(_cli())
