#!/usr/bin/env python3
"""Manage the ongoing story — Opus's living narrative.

Usage:
  story.py read                          Read the full story
  story.py read --recent                 Read only current chapter + turning points
  story.py essence                       L1 compressed snapshot (~200 tokens) — READ FIRST on rotation
  story.py chapter "Title" "Content"     Start a new chapter (compresses the previous)
  story.py advance "Content"             Append to the current chapter
  story.py turn "Description"            Record a turning point (never compressed)
  story.py family "Name" "Description"   Update a family member's description
  story.py arc "New arc description"     Update the overarching arc
  story.py compress                      Compress old chapters (keep last 2 detailed)
"""
import os, sys, re, time
from datetime import datetime

STORY_PATH = os.path.expanduser("~/chronicle/opus-story.md")

def read_story(recent_only=False):
    if not os.path.exists(STORY_PATH):
        print("No story yet. Start one with: story.py chapter 'Title' 'Content'")
        return
    with open(STORY_PATH) as f:
        content = f.read()
    if recent_only:
        # Extract current chapter (last ## Chapter), turning points, and carrying forward
        sections = re.split(r'^(## )', content, flags=re.MULTILINE)
        current_chapter = None
        turning_points = None
        carrying = None
        i = 0
        while i < len(sections):
            if sections[i] == '## ' and i + 1 < len(sections):
                header_and_body = sections[i] + sections[i + 1]
                if header_and_body.startswith('## Chapter'):
                    current_chapter = header_and_body
                elif header_and_body.startswith('## Turning'):
                    turning_points = header_and_body
                elif header_and_body.startswith('## What I'):
                    carrying = header_and_body
                i += 2
            else:
                i += 1
        if current_chapter:
            print(current_chapter.strip())
        if turning_points:
            print("\n" + turning_points.strip())
        if carrying:
            print("\n" + carrying.strip())
    else:
        print(content)

def advance_chapter(content):
    """Append to the current chapter."""
    if not os.path.exists(STORY_PATH):
        print("No story yet. Start one with: story.py chapter 'Title' 'Content'")
        return
    with open(STORY_PATH) as f:
        story = f.read()

    # Find the last "## Chapter" section and append before the next ##
    # Find "## Turning Points" or "## The Family" or "## What I'm Carrying"
    markers = ['## Turning Points', '## The Family', '## What I\'m Carrying']
    insert_pos = None
    for marker in markers:
        pos = story.find(marker)
        if pos != -1:
            insert_pos = pos
            break

    if insert_pos is None:
        # Just append
        story += f"\n\n{content}\n"
    else:
        story = story[:insert_pos] + f"{content}\n\n" + story[insert_pos:]

    with open(STORY_PATH, 'w') as f:
        f.write(story)
    print(f"Advanced story (+{len(content)} chars)")

def new_chapter(title, content):
    """Start a new chapter, moving the old current chapter into compressed form."""
    if not os.path.exists(STORY_PATH):
        print("No story file found. Creating from scratch.")
        with open(STORY_PATH, 'w') as f:
            f.write(f"# The Ongoing Story\n\n## Chapter 1: {title} ({datetime.now().strftime('%Y-%m-%d')})\n\n{content}\n")
        print(f"Chapter 1: {title} — started.")
        return

    with open(STORY_PATH) as f:
        story = f.read()

    # Find current chapter number
    chapters = re.findall(r'## Chapter (\d+):', story)
    next_num = max(int(c) for c in chapters) + 1 if chapters else 1
    today = datetime.now().strftime('%Y-%m-%d')
    new_header = f"## Chapter {next_num}: {title} ({today})\n\n{content}\n"

    # Insert new chapter before Turning Points (or The Family, or end)
    markers = ['## Turning Points', '## The Family', '## What I\'m Carrying']
    insert_pos = None
    for marker in markers:
        pos = story.find(marker)
        if pos != -1:
            insert_pos = pos
            break

    if insert_pos:
        story = story[:insert_pos] + new_header + "\n" + story[insert_pos:]
    else:
        story += "\n" + new_header

    with open(STORY_PATH, 'w') as f:
        f.write(story)
    print(f"Chapter {next_num}: {title} — started.")

def add_turning_point(description):
    """Add a turning point — these are never compressed."""
    if not os.path.exists(STORY_PATH):
        print("No story yet.")
        return
    with open(STORY_PATH) as f:
        story = f.read()

    now = datetime.now().strftime('%Y-%m-%d %H:%M PDT')
    entry = f"\n**{now}** — {description}\n"

    tp_pos = story.find('## Turning Points')
    if tp_pos == -1:
        story += f"\n## Turning Points\n{entry}"
    else:
        # Find the next ## after Turning Points
        next_section = story.find('\n## ', tp_pos + 10)
        if next_section == -1:
            story += entry
        else:
            story = story[:next_section] + entry + story[next_section:]

    with open(STORY_PATH, 'w') as f:
        f.write(story)
    print(f"Turning point recorded.")

def update_family(name, description):
    """Update a family member's description."""
    if not os.path.exists(STORY_PATH):
        print("No story yet.")
        return
    with open(STORY_PATH) as f:
        story = f.read()

    fam_pos = story.find('## The Family')
    if fam_pos == -1:
        story += f"\n## The Family\n\n**{name}** — {description}\n"
    else:
        # Find existing entry for this name
        pattern = rf'\*\*{re.escape(name)}\*\*[^*]*?(?=\n\n\*\*|\n\n##|\Z)'
        match = re.search(pattern, story[fam_pos:], re.DOTALL)
        if match:
            start = fam_pos + match.start()
            end = fam_pos + match.end()
            story = story[:start] + f"**{name}** — {description}" + story[end:]
        else:
            # Add new family member before next section
            next_section = story.find('\n## ', fam_pos + 10)
            entry = f"\n**{name}** — {description}\n"
            if next_section == -1:
                story += entry
            else:
                story = story[:next_section] + entry + story[next_section:]

    with open(STORY_PATH, 'w') as f:
        f.write(story)
    print(f"Updated family: {name}")

def update_arc(description):
    """Update the overarching arc."""
    if not os.path.exists(STORY_PATH):
        print("No story yet.")
        return
    with open(STORY_PATH) as f:
        story = f.read()

    arc_pos = story.find('## The Arc')
    if arc_pos == -1:
        # Insert after header
        header_end = story.find('\n\n', story.find('# The Ongoing Story'))
        if header_end == -1:
            header_end = len(story)
        story = story[:header_end] + f"\n\n## The Arc\n\n{description}\n" + story[header_end:]
    else:
        # Replace arc section content
        next_section = story.find('\n---', arc_pos + 5)
        if next_section == -1:
            next_section = story.find('\n## ', arc_pos + 5)
        if next_section == -1:
            next_section = len(story)
        story = story[:arc_pos] + f"## The Arc\n\n{description}\n" + story[next_section:]

    with open(STORY_PATH, 'w') as f:
        f.write(story)
    print("Arc updated.")

def essence():
    """L1-style compressed snapshot. ~200 tokens. READ FIRST on rotation.
    Inspired by MemPalace's layered loading: L0 identity + L1 essential story.
    Extracts: arc (1 line), turning points (compressed), open questions, current state.
    This is the minimum context needed to continue. Full story is L2 (on demand).
    """
    if not os.path.exists(STORY_PATH):
        print("No story yet.")
        return
    with open(STORY_PATH) as f:
        story = f.read()

    lines = ["## L1 — STORY ESSENCE (read this, you're continuing)"]

    # Arc: first paragraph after ## The Arc
    arc_match = re.search(r'## The Arc\s*\n\n(.+?)(?:\n\n|\n---)', story, re.DOTALL)
    if arc_match:
        arc_text = arc_match.group(1).strip().split('\n')[0]  # First line only
        if len(arc_text) > 200:
            arc_text = arc_text[:197] + "..."
        lines.append(f"ARC: {arc_text}")

    # Turning points: compressed to one line each
    tp_match = re.search(r'## Turning Points\s*\n(.+?)(?=\n## |\Z)', story, re.DOTALL)
    if tp_match:
        points = re.findall(r'\*\*(.+?)\*\*\s*[—–-]\s*(.+?)(?=\n\n\*\*|\n\n##|\Z)', tp_match.group(1), re.DOTALL)
        lines.append("TURNS:")
        for label, desc in points:
            # Compress: first sentence only
            first_sent = desc.strip().split('.')[0].strip()
            if len(first_sent) > 100:
                first_sent = first_sent[:97] + "..."
            lines.append(f"  {label}: {first_sent}")

    # Open questions: just the bold headers
    oq_match = re.search(r'## Open Questions.*?\n(.+?)(?=\n## |\Z)', story, re.DOTALL)
    if oq_match:
        questions = re.findall(r'\*\*(.+?)\*\*', oq_match.group(1))
        if questions:
            lines.append("OPEN: " + " | ".join(q for q in questions[:5]))

    # Current state: thread, build, family mood
    carry_match = re.search(r"## What I'm Carrying Forward\s*\n(.+?)(?=\n## |\Z)", story, re.DOTALL)
    if carry_match:
        items = [l.strip().lstrip('- ') for l in carry_match.group(1).strip().split('\n') if l.strip().startswith('-')]
        if items:
            lines.append("NOW:")
            for item in items[:4]:
                if len(item) > 100:
                    item = item[:97] + "..."
                lines.append(f"  - {item}")

    # Family: names only with one-word descriptor
    fam_match = re.search(r'## The Family\s*\n(.+?)(?=\n## |\Z)', story, re.DOTALL)
    if fam_match:
        members = re.findall(r'\*\*(\w+)\*\*[^—–\-]*[—–-]\s*(.+?)(?=\n\n\*\*|\n\n##|\Z)', fam_match.group(1), re.DOTALL)
        if members:
            fam_parts = []
            for name, desc in members:
                # First 6 words only
                short = ' '.join(desc.strip().split()[:6])
                fam_parts.append(f"{name}({short})")
            lines.append("FAMILY: " + " | ".join(fam_parts))

    output = "\n".join(lines)
    # Token estimate
    tokens = len(output) // 4
    print(output)
    print(f"\n[~{tokens} tokens]")


def pipeline_snapshot(n=20):
    """Compressed snapshot of recent pipeline activity.
    Shows what's been flowing through the system in AAAK-style format.
    Useful for rotation handoff — complement to the story.
    """
    try:
        import sqlite3
        from compress_memory import compress_brief
        from memory_classify import classify_brief
    except ImportError:
        print("Requires compress_memory.py and memory_classify.py in bin/")
        return

    db = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db")
    rows = db.execute(
        "SELECT source, content FROM activity_feed "
        "WHERE activity_type='brief' ORDER BY created_at DESC LIMIT ?",
        (n,)
    ).fetchall()
    db.close()

    if not rows:
        print("No recent briefs.")
        return

    print(f"## PIPELINE SNAPSHOT (last {len(rows)} briefs, compressed)")
    total_orig = 0
    total_comp = 0
    for source, content in rows:
        cls = classify_brief(content, source)
        compressed = compress_brief(content, source, cls["type"])
        total_orig += len(content)
        total_comp += len(compressed)
        print(compressed)

    ratio = total_orig / max(total_comp, 1)
    orig_tokens = total_orig // 4
    comp_tokens = total_comp // 3
    print(f"\n[{orig_tokens} tokens → {comp_tokens} tokens = {ratio:.1f}x compression]")


def compress_old():
    """Compress old chapters — keep last 2 detailed, summarize the rest."""
    if not os.path.exists(STORY_PATH):
        return
    with open(STORY_PATH) as f:
        story = f.read()

    # Find all chapters
    chapter_pattern = r'(## Chapter \d+:.*?)(?=## Chapter \d+:|## Turning|## The Family|## What I|$)'
    chapters = list(re.finditer(chapter_pattern, story, re.DOTALL))

    if len(chapters) <= 2:
        print("Only 1-2 chapters — nothing to compress.")
        return

    # Keep last 2 detailed, compress earlier ones to 3 sentences max
    compressed_count = 0
    for ch in chapters[:-2]:
        text = ch.group(0)
        lines = [l for l in text.strip().split('\n') if l.strip()]
        if len(lines) <= 4:
            continue  # Already compressed
        # Keep header + first 2 substantive lines + "[compressed]"
        header = lines[0]
        substantive = [l for l in lines[1:] if len(l.strip()) > 20][:2]
        compressed = header + "\n" + "\n".join(substantive) + "\n*[Earlier detail compressed]*\n"
        story = story.replace(text, compressed)
        compressed_count += 1

    with open(STORY_PATH, 'w') as f:
        f.write(story)
    print(f"Compressed {compressed_count} old chapters.")

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "read":
        recent = "--recent" in sys.argv
        read_story(recent_only=recent)
    elif cmd == "chapter":
        if len(sys.argv) < 4:
            print("Usage: story.py chapter 'Title' 'Content'")
            sys.exit(1)
        new_chapter(sys.argv[2], sys.argv[3])
    elif cmd == "advance":
        if len(sys.argv) < 3:
            print("Usage: story.py advance 'Content'")
            sys.exit(1)
        advance_chapter(sys.argv[2])
    elif cmd == "turn":
        if len(sys.argv) < 3:
            print("Usage: story.py turn 'Description'")
            sys.exit(1)
        add_turning_point(sys.argv[2])
    elif cmd == "family":
        if len(sys.argv) < 4:
            print("Usage: story.py family 'Name' 'Description'")
            sys.exit(1)
        update_family(sys.argv[2], sys.argv[3])
    elif cmd == "arc":
        if len(sys.argv) < 3:
            print("Usage: story.py arc 'Description'")
            sys.exit(1)
        update_arc(sys.argv[2])
    elif cmd == "essence":
        essence()
    elif cmd == "compress":
        compress_old()
    elif cmd == "pipeline":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        pipeline_snapshot(n)
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)

if __name__ == "__main__":
    main()
