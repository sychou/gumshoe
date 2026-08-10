#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.32",
#     "youtube-transcript-api>=1.0",
#     "markdownify>=0.13",
#     "tomli>=2.0; python_version < '3.11'",
# ]
# ///
"""
Gumshoe — fetches external content and files it as markdown into an Obsidian vault.

Three phases, independently invocable:
  1. Scan    — discover candidate items from every source into the queue.
  2. Prepare — dedup, drop already-held, sort newest-first, persist to state.
  3. Fetch   — acquire content, write markdown, advance cursors.

`run` does all three. `scan` does 1+2 only. `fetch` does 3 from the
persisted queue.

Usage:
    gumshoe run                 # scan + fetch: all sources
    gumshoe run <source-name>   # scan + fetch: one source only
    gumshoe scan                # scan only: discover and queue, no fetching
    gumshoe scan <source-name>  # scan one source only
    gumshoe fetch               # fetch only: process the persisted queue
    gumshoe add <url>           # append a one-off URL to the queue
    gumshoe status              # fetch queue, cursors, last run, failures
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree

try:
    import tomllib  # py311+
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

import requests

CONFIG_DIR = Path.home() / ".config" / "gumshoe"
CONFIG_FILE = CONFIG_DIR / "config.toml"
STATE_FILE = CONFIG_DIR / "state.json"
QUEUE_FILE = CONFIG_DIR / "queue.txt"
LOCK_FILE = CONFIG_DIR / "gumshoe.lock"
DEFAULT_VAULT_ROOT = Path.home() / "Vaults" / "Gumshoe"

# YouTube rate limits: 10/hr, 30s apart.
CAPTION_SPACING = 30.0
CAPTION_HOURLY_LIMIT = 10
FEED_SPACING = 5.0
FEED_ATTEMPTS = 4
MIN_DURATION = 300  # skip videos shorter than 5 minutes

ATOM_NS = "{http://www.w3.org/2005/Atom}"
YT_NS = "{http://www.youtube.com/xml/schemas/2015}"


# ──────────────────────────────────────────────────────────────────────
# Config & state
# ──────────────────────────────────────────────────────────────────────

@dataclass
class YouTubeSource:
    name: str
    channel_id: str
    slug: str = ""

    def __post_init__(self) -> None:
        if not self.slug:
            self.slug = slugify(self.name)


@dataclass
class NewsletterSource:
    name: str
    sender: str
    subject: str = ""
    slug: str = ""
    account: str = ""  # overrides global account if set

    def __post_init__(self) -> None:
        if not self.slug:
            self.slug = slugify(self.name)


@dataclass
class Config:
    vault_root: Path
    youtube: list[YouTubeSource] = field(default_factory=list)
    newsletters: list[NewsletterSource] = field(default_factory=list)
    account: str = "choufam"
    newsletter_window_days: int = 1
    tailscale_exit_nodes: list[str] = field(default_factory=list)


def load_config() -> Config:
    if not CONFIG_FILE.exists():
        raise SystemExit(f"No config found at {CONFIG_FILE}. See sample at end of this script.")
    with CONFIG_FILE.open("rb") as f:
        raw = tomllib.load(f)
    vault = Path(raw.get("vault_root", DEFAULT_VAULT_ROOT)).expanduser()
    account = raw.get("account", "choufam")
    window = raw.get("newsletter_window_days", 1)
    yt: list[YouTubeSource] = []
    for src in raw.get("youtube", []):
        yt.append(YouTubeSource(
            name=src["name"],
            channel_id=resolve_channel_id(src["channel_id"]),
        ))
    nl: list[NewsletterSource] = []
    for src in raw.get("newsletter", []):
        nl.append(NewsletterSource(
            name=src["name"],
            sender=src["sender"],
            subject=src.get("subject", ""),
            account=src.get("account", ""),
        ))
    return Config(vault_root=vault, youtube=yt, newsletters=nl,
                  account=account, newsletter_window_days=window,
                  tailscale_exit_nodes=raw.get("tailscale_exit_nodes", []))


def resolve_channel_id(value: str) -> str:
    """Accept UC... raw, https://youtube.com/channel/UC..., or handle URLs.
    For handle URLs (/c/Name or /@handle) we resolve via the feed by trying
    the known handle→channel_id lookup endpoints. v1 keeps this simple:
    accept UC... or channel URLs only."""
    v = value.strip()
    if v.startswith("UC") and len(v) >= 24:
        return v
    m = re.search(r"channel/(UC[A-Za-z0-9_-]{22})", v)
    if m:
        return m.group(1)
    raise SystemExit(
        f"Could not resolve channel_id from {value!r}. "
        "Provide the UC... id or a youtube.com/channel/UC... URL."
    )


def load_state() -> dict:
    if STATE_FILE.exists():
        with STATE_FILE.open() as f:
            return json.load(f)
    return {"sources": {}, "queue": [], "last_run": None, "failures": []}


def save_state(state: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    tmp.replace(STATE_FILE)


def acquire_lock() -> None:
    """Raise SystemExit if another run is in progress. Stale locks (process
    gone) are reclaimed."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
        except (ValueError, OSError):
            pid = None  # corrupt or unreadable lock — reclaim
        if pid is not None:
            try:
                os.kill(pid, 0)  # signal 0: check if alive
            except ProcessLookupError:
                pass  # stale lock — reclaim
            except PermissionError:
                # Process exists but is owned by another user — still running.
                raise SystemExit(f"Another gumshoe run is in progress (pid {pid}). Exiting.")
            else:
                raise SystemExit(f"Another gumshoe run is in progress (pid {pid}). Exiting.")
    LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")


def release_lock() -> None:
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass


# ──────────────────────────────────────────────────────────────────────
# Slug, path, frontmatter
# ──────────────────────────────────────────────────────────────────────

def slugify(text: str, limit: int = 60) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    if len(slug) > limit:
        slug = slug[:limit].rsplit("-", 1)[0]
    return slug or "untitled"


def safe_dir(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_") or "source"


def output_path(vault_root: Path, slug: str, date_str: str, title: str,
                item_id: str) -> Path:
    folder = vault_root / safe_dir(slug)
    folder.mkdir(parents=True, exist_ok=True)
    base = f"{date_str}-{slugify(title)}"
    path = folder / f"{base}.md"
    n = 2
    while path.exists():
        head = path.read_text(encoding="utf-8")[:600]
        if re.search(rf'item_id:\s*"?{re.escape(item_id)}"?', head):
            return path
        path = folder / f"{base}-{n}.md"
        n += 1
    return path


def frontmatter(meta: dict) -> str:
    lines = ["---"]
    for k, v in meta.items():
        if isinstance(v, (list, dict)):
            lines.append(f"{k}: {json.dumps(v, ensure_ascii=False)}")
        elif isinstance(v, bool):
            lines.append(f"{k}: {'true' if v else 'false'}")
        elif isinstance(v, (int, float)):
            lines.append(f"{k}: {v}")
        elif v is None:
            lines.append(f"{k}:")
        else:
            es = str(v).replace('"', '\\"')
            lines.append(f'{k}: "{es}"')
    lines.append("---")
    return "\n".join(lines) + "\n"


def write_markdown(path: Path, meta: dict, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(frontmatter(meta) + "\n" + body + "\n", encoding="utf-8")
    return path


# ──────────────────────────────────────────────────────────────────────
# Item model
# ──────────────────────────────────────────────────────────────────────

@dataclass
class Item:
    """A queued candidate item. Source-agnostic."""
    source: str            # source slug
    source_name: str       # human name
    source_type: str       # "youtube" | "newsletter"
    item_id: str           # video ID, message ID
    title: str
    url: str
    published: datetime
    fetcher: str           # which fetcher handles this: "youtube" | "newsletter"
    extra: dict = field(default_factory=dict)


def serialize_queue(items: list[Item]) -> list[dict]:
    return [
        {"source": i.source, "source_name": i.source_name,
         "source_type": i.source_type, "item_id": i.item_id,
         "title": i.title, "url": i.url,
         "published": i.published.isoformat(),
         "fetcher": i.fetcher, "extra": i.extra}
        for i in items
    ]


def deserialize_queue(raw: list[dict]) -> list[Item]:
    return [
        Item(source=q["source"], source_name=q["source_name"],
             source_type=q["source_type"], item_id=q["item_id"],
             title=q["title"], url=q["url"],
             published=datetime.fromisoformat(q["published"]),
             fetcher=q["fetcher"], extra=q.get("extra", {}))
        for q in raw
    ]


# ──────────────────────────────────────────────────────────────────────
# YouTube fetcher
# ──────────────────────────────────────────────────────────────────────

_last_feed_fetch = 0.0


def fetch_feed(url: str) -> list[Item]:
    """Poll a YouTube channel Atom feed. Cheap; no caption calls."""
    global _last_feed_fetch
    wait = FEED_SPACING - (time.monotonic() - _last_feed_fetch)
    if wait > 0:
        time.sleep(wait)
    delay = 4.0
    failure: Exception | None = None
    for attempt in range(FEED_ATTEMPTS):
        if attempt:
            time.sleep(delay)
            delay *= 2
        _last_feed_fetch = time.monotonic()
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            break
        except Exception as e:  # noqa: BLE001
            failure = e
            if attempt < FEED_ATTEMPTS - 1:
                print(f"  feed fetch failed ({type(e).__name__}), retrying")
    else:
        raise failure  # type: ignore[misc]

    root = ElementTree.fromstring(resp.content)
    items: list[Item] = []
    for entry in root.findall(f"{ATOM_NS}entry"):
        vid_el = entry.find(f"{YT_NS}videoId")
        if vid_el is None or not vid_el.text:
            continue
        vid = vid_el.text
        title_el = entry.find(f"{ATOM_NS}title")
        title = (title_el.text if title_el is not None else None) or "Untitled"
        pub_el = entry.find(f"{ATOM_NS}published")
        try:
            published = datetime.fromisoformat(pub_el.text.replace("Z", "+00:00"))  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            published = datetime.now(timezone.utc)
        author_el = entry.find(f"{ATOM_NS}author").find(f"{ATOM_NS}name") if entry.find(f"{ATOM_NS}author") is not None else None  # type: ignore[union-attr]
        author = (author_el.text or "") if author_el is not None else ""  # type: ignore[union-attr]
        items.append(Item(
            source="",
            source_name=author,
            source_type="youtube",
            item_id=vid,
            title=title,
            url=f"https://www.youtube.com/watch?v={vid}",
            published=published,
            fetcher="youtube",
            extra={"author": author},
        ))
    return items


def video_duration(video_id: str) -> int | None:
    """Fetch video duration in seconds from the watch page. Cheap but costs
    a network call, so only called for items not already in the vault."""
    try:
        resp = requests.get(
            f"https://www.youtube.com/watch?v={video_id}",
            timeout=30, headers={"User-Agent": "Mozilla/5.0"},
        )
        m = re.search(r'"lengthSeconds":"(\d+)"', resp.text)
        return int(m.group(1)) if m else None
    except Exception:  # noqa: BLE001
        return None


_caption_times: list[float] = []
_last_caption_fetch = 0.0


# ──────────────────────────────────────────────────────────────────────
# Tailscale exit-node rotation
# ──────────────────────────────────────────────────────────────────────

def tailscale_available() -> bool:
    import shutil
    return bool(shutil.which("tailscale"))


def set_exit_node(node: str | None) -> bool:
    """Set or clear the Tailscale exit node. Returns True on success."""
    if not tailscale_available():
        return False
    cmd = ["tailscale", "set"]
    if node:
        cmd += ["--exit-node", node]
        print(f"  [tailscale] switching exit node to {node}")
    else:
        cmd += ["--exit-node", ""]
        print("  [tailscale] clearing exit node")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        print(f"  [tailscale] failed: {proc.stderr.strip()[:200]}", file=sys.stderr)
        return False
    return True


def caption_rate_ok() -> bool:
    """True if we can make another caption fetch under the hourly cap.
    Times are wall-clock epoch seconds so the cap persists across runs
    via state["caption_times"]."""
    cutoff = time.time() - 3600.0
    while _caption_times and _caption_times[0] < cutoff:
        _caption_times.pop(0)
    return len(_caption_times) < CAPTION_HOURLY_LIMIT


def fetch_captions(item: Item) -> tuple[str | None, str]:
    """Return (text, status). status is one of:
      "ok"          — text holds the transcript
      "no_captions" — the video has no English captions (permanent; never retry)
      "blocked"     — YouTube is rate-limiting this IP; rotate or halt
      "error"       — transient failure (network, etc.); retry next run
    """
    global _last_caption_fetch
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api.formatters import TextFormatter

    wait = CAPTION_SPACING - (time.monotonic() - _last_caption_fetch)
    if wait > 0:
        time.sleep(wait)
    _last_caption_fetch = time.monotonic()
    _caption_times.append(time.time())
    try:
        transcript = YouTubeTranscriptApi().fetch(item.item_id, languages=["en", "en-US"])
        return TextFormatter().format_transcript(transcript), "ok"
    except Exception as e:  # noqa: BLE001
        name = type(e).__name__
        if "Blocked" in name or "Ip" in name:
            return None, "blocked"
        if name in ("TranscriptsDisabled", "NoTranscriptFound", "NoTranscriptAvailable",
                    "VideoUnavailable", "VideoUnplayable", "AgeRestricted"):
            return None, "no_captions"
        return None, "error"


def fetch_video_one_off(video_id: str) -> Item:
    """Resolve a pasted YouTube video URL into an Item via the oEmbed endpoint.
    Cheap, no caption call."""
    oembed = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    resp = requests.get(oembed, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    title = data.get("title", video_id)
    author = data.get("author_name", "YouTube")
    return Item(
        source="one-off",
        source_name=author,
        source_type="youtube",
        item_id=video_id,
        title=title,
        url=f"https://www.youtube.com/watch?v={video_id}",
        published=datetime.now(timezone.utc),
        fetcher="youtube",
        extra={"author": author, "one_off": True},
    )


def extract_video_id(text: str) -> str | None:
    m = re.search(r"(?:v=|youtu\.be/|/embed/)([A-Za-z0-9_-]{11})", text)
    return m.group(1) if m else None


# ──────────────────────────────────────────────────────────────────────
# Newsletter fetcher (gog)
# ──────────────────────────────────────────────────────────────────────

def gog_search(query: str, account: str, max_results: int = 20) -> list[dict]:
    """Run `gog gmail search` and return parsed JSON rows."""
    cmd = ["gog", "gmail", "search", query, "-a", account,
           "--max", str(max_results), "-j", "--results-only"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, check=False)
    except subprocess.TimeoutExpired:
        print("  gog search timed out", file=sys.stderr)
        return []
    if proc.returncode != 0:
        print(f"  gog search failed: {proc.stderr.strip()[:200]}", file=sys.stderr)
        return []
    try:
        return json.loads(proc.stdout) if proc.stdout.strip() else []
    except json.JSONDecodeError:
        return []


def gog_get(message_id: str, account: str) -> dict | None:
    cmd = ["gog", "gmail", "get", message_id, "-a", account,
           "--format", "full", "-j", "--results-only"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, check=False)
    except subprocess.TimeoutExpired:
        print("  gog get timed out", file=sys.stderr)
        return None
    if proc.returncode != 0:
        return None
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None


def html_to_markdown(html: str) -> str:
    """Use defuddle to extract main content as markdown. Falls back to
    markdownify if defuddle (Node.js) is not available."""
    import shutil
    if shutil.which("npx"):
        try:
            proc = subprocess.run(
                ["npx", "--yes", "defuddle", "parse", "--markdown"],
                input=html, capture_output=True, text=True, timeout=30, check=False,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                return proc.stdout.strip()
            # fall through to markdownify
        except Exception:  # noqa: BLE001,S110
            pass

    from markdownify import markdownify as md
    html = re.sub(r"<head\b[^>]*>.*?</head>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<style\b[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<script\b[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"</?t(?:able|body|head|d|r|h)\b[^>]*>", "\n", html, flags=re.IGNORECASE)
    out = md(html, heading_style="ATX", strip=["img", "style", "script"])
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


def body_to_markdown(body: str) -> str:
    """If body looks like HTML, convert; else return as plain text."""
    if "<" in body and ">" in body and re.search(r"</?(html|p|div|span|br|table|a|head)\b", body, re.IGNORECASE):
        return html_to_markdown(body)
    return body.strip()


def latest_vault_date(vault_root: Path, slug: str) -> str | None:
    """Return the YYYY/MM/DD of the most recent file for a source, or None."""
    folder = vault_root / safe_dir(slug)
    if not folder.exists():
        return None
    dates = []
    for md in folder.glob("*.md"):
        m = re.match(r"(\d{4}-\d{2}-\d{2})-", md.name)
        if m:
            dates.append(m.group(1))
    if not dates:
        return None
    return max(dates)


def discover_newsletters(src: NewsletterSource, account: str,
                         window_days: int, vault_root: Path) -> list[Item]:
    """Find newsletter messages from this sender. Look back to the most
    recent one already in the vault; if none, use window_days as default."""
    latest = latest_vault_date(vault_root, src.slug)
    if latest:
        q = f"from:{src.sender} after:{latest}"
    else:
        q = f"from:{src.sender} newer_than:{window_days}d"
    if src.subject:
        q += f' subject:"{src.subject}"'
    rows = gog_search(q, account)
    items: list[Item] = []
    for r in rows:
        try:
            published = datetime.fromisoformat(r["internalDateIso"])
        except (KeyError, ValueError):
            published = datetime.now(timezone.utc)
        items.append(Item(
            source=src.slug,
            source_name=src.name,
            source_type="newsletter",
            item_id=r["id"],
            title=r.get("subject", src.name),
            url=f"gmail://{account}/{r['id']}",
            published=published,
            fetcher="newsletter",
            extra={"account": account, "from": r.get("from", src.sender)},
        ))
    return items


def fetch_newsletter_body(item: Item, default_account: str) -> str | None:
    """Fetch and convert the message body. Does NOT archive — the caller
    archives only after the vault note is safely written."""
    acct = item.extra.get("account") or default_account
    msg = gog_get(item.item_id, acct)
    if not msg:
        return None
    body = msg.get("body", "")
    if not body:
        return None
    return body_to_markdown(body)


def archive_newsletter(message_id: str, account: str) -> None:
    """Remove INBOX label to archive the newsletter. Best-effort — failure
    is logged but doesn't block the fetch."""
    cmd = ["gog", "gmail", "messages", "modify", message_id,
           "-a", account, "--remove", "INBOX", "-y"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    except subprocess.TimeoutExpired:
        print("  [archive] timed out", file=sys.stderr)
        return
    if proc.returncode != 0:
        print(f"  [archive] failed: {proc.stderr.strip()[:120]}", file=sys.stderr)


# ──────────────────────────────────────────────────────────────────────
# Queue (one-off URL file)
# ──────────────────────────────────────────────────────────────────────

def read_queue_file() -> list[str]:
    if not QUEUE_FILE.exists():
        return []
    lines = QUEUE_FILE.read_text(encoding="utf-8").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")]


def consume_queue_item(url: str) -> None:
    """Remove a successfully fetched URL from the queue file."""
    if not QUEUE_FILE.exists():
        return
    remaining = [ln for ln in QUEUE_FILE.read_text(encoding="utf-8").splitlines()
                 if ln.strip() and ln.strip() != url]
    QUEUE_FILE.write_text("\n".join(remaining) + ("\n" if remaining else ""), encoding="utf-8")


def resolve_one_off(url: str) -> Item | None:
    """Detect URL type and route to the right fetcher's discovery."""
    if vid := extract_video_id(url):
        try:
            item = fetch_video_one_off(vid)
        except Exception as e:  # noqa: BLE001
            print(f"  could not resolve YouTube URL {url}: {e}", file=sys.stderr)
            return None
        # Remember the raw pasted line so consume_queue_item can remove it
        # even when it isn't the canonical watch URL (youtu.be, &t=30s, …).
        item.extra["queue_line"] = url
        return item
    # Future: other URL types (article, podcast episode).
    print(f"  unrecognized one-off URL: {url}", file=sys.stderr)
    return None


# ──────────────────────────────────────────────────────────────────────
# State helpers (per-source cursors / id sets)
# ──────────────────────────────────────────────────────────────────────

def fetched_ids(state: dict, source_slug: str) -> set[str]:
    return set(state.setdefault("sources", {}).setdefault(source_slug, {}).get("fetched", []))


def mark_fetched(state: dict, source_slug: str, item_id: str) -> None:
    s = state.setdefault("sources", {}).setdefault(source_slug, {})
    s.setdefault("fetched", []).append(item_id)
    s["last_fetch"] = datetime.now(timezone.utc).isoformat()


def skipped_ids(state: dict, source_slug: str) -> set[str]:
    """Items permanently skipped (e.g. captions disabled) — never retried."""
    return set(state.setdefault("sources", {}).setdefault(source_slug, {}).get("skipped", []))


def mark_skipped(state: dict, source_slug: str, item_id: str) -> None:
    s = state.setdefault("sources", {}).setdefault(source_slug, {})
    skipped = s.setdefault("skipped", [])
    if item_id not in skipped:
        skipped.append(item_id)


def record_failure(state: dict, source_slug: str, item_id: str, detail: str) -> None:
    f = state.setdefault("failures", [])
    f.append({
        "source": source_slug,
        "item_id": item_id,
        "detail": detail[:300],
        "at": datetime.now(timezone.utc).isoformat(),
    })


# ──────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────

def already_have(vault_root: Path, slug: str, item_id: str) -> bool:
    folder = vault_root / safe_dir(slug)
    if not folder.exists():
        return False
    for md in folder.glob("*.md"):
        head = md.read_text(encoding="utf-8")[:600]
        if re.search(rf'item_id:\s*"?{re.escape(item_id)}"?', head):
            return True
    return False


def emit_youtube(config: Config, item: Item, body: str) -> Path:
    meta = {
        "source": item.source_name,
        "source_type": "youtube",
        "title": item.title,
        "url": item.url,
        "published": item.published.date().isoformat(),
        "fetched": datetime.now(timezone.utc).isoformat(),
        "item_id": item.item_id,
        "author": item.extra.get("author", ""),
    }
    slug = item.source or slugify(item.source_name)
    return write_markdown(
        output_path(config.vault_root, slug, item.published.date().isoformat(),
                    item.title, item.item_id),
        meta, body,
    )


def emit_newsletter(config: Config, item: Item, body: str) -> Path:
    meta = {
        "source": item.source_name,
        "source_type": "newsletter",
        "title": item.title,
        "url": item.url,
        "published": item.published.date().isoformat(),
        "fetched": datetime.now(timezone.utc).isoformat(),
        "item_id": item.item_id,
        "from": item.extra.get("from", ""),
    }
    slug = item.source or slugify(item.source_name)
    return write_markdown(
        output_path(config.vault_root, slug, item.published.date().isoformat(),
                    item.title, item.item_id),
        meta, body,
    )


def scan(config: Config, state: dict, only: str | None = None) -> list[Item]:
    """Phase 1: discover all items from every source into the queue. No filtering."""
    queue: list[Item] = []

    for src in config.youtube:
        if only and only != src.slug and only != src.name:
            continue
        print(f"[scan] YouTube: {src.name}")
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={src.channel_id}"
        try:
            vids = fetch_feed(feed_url)
        except Exception as e:  # noqa: BLE001
            print(f"  feed fetch failed: {e}", file=sys.stderr)
            record_failure(state, src.slug, "", f"feed fetch: {e}")
            continue
        kept = []
        skipped = skipped_ids(state, src.slug)
        for v in vids:
            v.source = src.slug
            v.source_name = src.name
            if v.item_id in skipped:
                continue
            # Skip Shorts and clips — check duration for items not in vault
            if not already_have(config.vault_root, src.slug, v.item_id):
                dur = video_duration(v.item_id)
                if dur is not None and dur < MIN_DURATION:
                    print(f"  skip ({dur}s): {v.title[:60]}")
                    continue
            kept.append(v)
        queue.extend(kept)
        print(f"  {len(kept)} in feed (after duration filter)")

    for src in config.newsletters:
        if only and only != src.slug and only != src.name:
            continue
        print(f"[scan] Newsletter: {src.name}")
        acct = src.account or config.account
        msgs = discover_newsletters(src, acct, config.newsletter_window_days,
                                     config.vault_root)
        queue.extend(msgs)
        print(f"  {len(msgs)} found")

    if not only:
        for url in read_queue_file():
            print(f"[scan] one-off: {url}")
            item = resolve_one_off(url)
            if item:
                queue.append(item)

    print(f"[scan] {len(queue)} items discovered")
    return queue


def prepare(vault: Path, queue: list[Item], state: dict) -> list[Item]:
    """Phase 2: dedup, drop already-held, sort newest-first, persist to state."""
    clean: list[Item] = []
    seen_ids: set[str] = set()
    for item in queue:
        slug = item.source or slugify(item.source_name)
        if item.item_id in seen_ids:
            continue
        seen_ids.add(item.item_id)
        if item.item_id in skipped_ids(state, slug):
            continue
        if already_have(vault, slug, item.item_id):
            if item.extra.get("one_off"):
                consume_queue_item(item.extra.get("queue_line", item.url))
            continue
        clean.append(item)

    clean.sort(key=lambda i: (i.fetcher != "newsletter", -i.published.timestamp()))

    state["queue"] = serialize_queue(clean)

    dropped = len(queue) - len(clean)
    print(f"[prepare] {len(clean)} to fetch ({dropped} dropped as dupes or already held)")
    return clean


def fetch(config: Config, state: dict, queue: list[Item]) -> tuple[int, int, int]:
    """Phase 3: fetch the queue with rate limits. Returns (fetched, deferred, failed).
    Newsletters have no rate limits and are always fetched. YouTube items are
    throttled; a block or cap stops YouTube processing but the queue is preserved
    for the next run."""
    vault = config.vault_root
    fetched_count = 0
    deferred = 0
    failed = 0
    exit_node_idx = 0
    using_exit_node = False
    youtube_halted = False
    remaining: list[Item] = []  # items deferred or failed, for next run

    # Seed the hourly caption cap from state so it holds across runs
    cutoff = time.time() - 3600.0
    _caption_times[:] = [t for t in state.get("caption_times", []) if t > cutoff]

    # If Tailscale exit nodes are configured, start with the first one
    if config.tailscale_exit_nodes and tailscale_available() and \
            set_exit_node(config.tailscale_exit_nodes[0]):
        using_exit_node = True
        exit_node_idx = 0

    try:
        for item in queue:
            slug = item.source or slugify(item.source_name)

            if item.fetcher == "youtube":
                if youtube_halted:
                    remaining.append(item)
                    deferred += 1
                    continue
                if not caption_rate_ok():
                    print(f"  [defer] {item.title} — hourly caption cap reached")
                    remaining.append(item)
                    deferred += 1
                    youtube_halted = True
                    continue
                print(f"[fetch] YouTube: {item.title}")
                text, status = fetch_captions(item)
                if status == "blocked":
                    # Try rotating to the next exit node, then retry once
                    if using_exit_node and exit_node_idx + 1 < len(config.tailscale_exit_nodes):
                        exit_node_idx += 1
                        next_node = config.tailscale_exit_nodes[exit_node_idx]
                        print(f"  YouTube IP block — rotating to {next_node}", file=sys.stderr)
                        record_failure(state, slug, item.item_id, f"YouTube IP block — rotating to {next_node}")
                        if set_exit_node(next_node):
                            _caption_times.clear()
                            text, status = fetch_captions(item)
                    if status == "blocked":
                        print("  YouTube IP block — halting YouTube; queue preserved", file=sys.stderr)
                        record_failure(state, slug, item.item_id, "YouTube IP block")
                        remaining.append(item)
                        deferred += 1
                        youtube_halted = True
                        continue
                if status == "no_captions":
                    print(f"  no captions — skipping {item.item_id} permanently")
                    record_failure(state, slug, item.item_id, "no captions")
                    mark_skipped(state, slug, item.item_id)
                    if item.extra.get("one_off"):
                        consume_queue_item(item.extra.get("queue_line", item.url))
                    failed += 1
                    continue
                if status == "error":
                    print(f"  caption fetch error — will retry next run: {item.item_id}", file=sys.stderr)
                    record_failure(state, slug, item.item_id, "caption fetch error (transient)")
                    remaining.append(item)
                    deferred += 1
                    continue
                path = emit_youtube(config, item, text)
                mark_fetched(state, slug, item.item_id)
                if item.extra.get("one_off"):
                    consume_queue_item(item.extra.get("queue_line", item.url))
                fetched_count += 1
                print(f"  wrote {path.relative_to(vault)}")

            elif item.fetcher == "newsletter":
                print(f"[fetch] Newsletter: {item.title}")
                body = fetch_newsletter_body(item, config.account)
                if body is None:
                    print(f"  could not fetch body — skipping {item.item_id}")
                    record_failure(state, slug, item.item_id, "no body")
                    failed += 1
                    continue
                path = emit_newsletter(config, item, body)
                # Only archive out of the inbox once the vault note exists
                archive_newsletter(item.item_id, item.extra.get("account") or config.account)
                mark_fetched(state, slug, item.item_id)
                fetched_count += 1
                print(f"  wrote {path.relative_to(vault)}")
    finally:
        if using_exit_node:
            set_exit_node(None)

    # Persist only remaining (deferred) items for the next run
    state["queue"] = serialize_queue(remaining)
    state["caption_times"] = list(_caption_times)

    return fetched_count, deferred, failed


def run(config: Config, state: dict, only: str | None = None,
        scan_only: bool = False, fetch_only: bool = False) -> int:
    if fetch_only:
        # Fetch from the persisted queue in state
        raw_q = state.get("queue", [])
        if not raw_q:
            print("[fetch] no queued items to fetch")
            state["last_run"] = datetime.now(timezone.utc).isoformat()
            save_state(state)
            return 0
        # Re-filter: drop items already held since the queue was built
        queue = prepare(config.vault_root, deserialize_queue(raw_q), state)
    else:
        # Scan + prepare. Carry the previously persisted queue along so
        # deferred items survive scoped runs and fresh scans (prepare's
        # dedup drops anything rediscovered).
        discovered = scan(config, state, only=only)
        discovered.extend(deserialize_queue(state.get("queue", [])))
        queue = prepare(config.vault_root, discovered, state)

    if scan_only:
        state["last_run"] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        print(f"\n[done] scanned {len(queue)} items queued for fetch")
        return 0

    fetched_count, deferred, failed = fetch(config, state, queue)
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    print(f"\n[done] fetched={fetched_count} deferred={deferred} failed={failed}")
    return 0


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def cmd_run(args: list[str]) -> int:
    config = load_config()
    state = load_state()
    only = args[0] if args else None
    acquire_lock()
    try:
        return run(config, state, only=only)
    finally:
        release_lock()


def cmd_scan(args: list[str]) -> int:
    config = load_config()
    state = load_state()
    only = args[0] if args else None
    acquire_lock()
    try:
        return run(config, state, only=only, scan_only=True)
    finally:
        release_lock()


def cmd_fetch(args: list[str]) -> int:
    config = load_config()
    state = load_state()
    acquire_lock()
    try:
        return run(config, state, fetch_only=True)
    finally:
        release_lock()


def cmd_add(args: list[str]) -> int:
    if not args:
        print("usage: gumshoe add <url>", file=sys.stderr)
        return 2
    url = args[0]
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with QUEUE_FILE.open("a") as f:
        f.write(url + "\n")
    print(f"queued: {url}")
    return 0


def cmd_status(args: list[str]) -> int:
    state = load_state()
    print(f"state: {STATE_FILE}")
    print(f"last run: {state.get('last_run', 'never')}")
    sources = state.get("sources", {})
    if sources:
        print("\nsources:")
        for slug, s in sorted(sources.items()):
            fetched = len(s.get("fetched", []))
            last = s.get("last_fetch", "—")
            print(f"  {slug}: {fetched} fetched, last {last}")
    q = state.get("queue", [])
    if q:
        print(f"\nfetch queue: {len(q)} items pending")
        for item in q[:10]:
            print(f"  {item['source_type']:10} {item.get('published', '?')[:10]}  {item['title'][:70]}")
        if len(q) > 10:
            print(f"  ... and {len(q) - 10} more")
    qlen = len(read_queue_file())
    print(f"\none-off URL queue: {qlen} pending ({QUEUE_FILE})")
    fails = state.get("failures", [])
    if fails:
        print("\nfailures (last 5):")
        for fr in fails[-5:]:
            print(f"  {fr.get('source', '?')}/{fr.get('item_id', '?')}: {fr.get('detail', '')[:120]}")
    return 0


def cmd_sample(args: list[str]) -> int:
    """Print a sample config to stdout."""
    sample = '''\
# ~/.config/gumshoe/config.toml
vault_root = "~/Vaults/Gumshoe"
account = "choufam"            # gog account for newsletters
newsletter_window_days = 1

[[youtube]]
name = "Example Channel"
channel_id = "UCxxxxxxxxxxxxxxxxxxxxxxxx"

[[youtube]]
name = "Another Show"
channel_id = "https://www.youtube.com/channel/UCxxxxxxxxxxxxxxxxxxxxxxxx"

[[newsletter]]
name = "Example Newsletter"
sender = "newsletter@example.com"
# subject = "Daily"            # optional subject match
# account = "isomer"           # per-source override (default: global account)

# Tailscale exit nodes to rotate through when YouTube blocks an IP.
# Only set on machines that are not exit nodes themselves. Omit to disable.
# tailscale_exit_nodes = ["us-chi-wg-301.mullvad.ts.net", "us-dal-wg-001.mullvad.ts.net"]
'''
    print(sample)
    return 0


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return 0
    cmd, *rest = argv
    if cmd == "run":
        return cmd_run(rest)
    if cmd == "scan":
        return cmd_scan(rest)
    if cmd == "fetch":
        return cmd_fetch(rest)
    if cmd == "add":
        return cmd_add(rest)
    if cmd == "status":
        return cmd_status(rest)
    if cmd == "sample":
        return cmd_sample(rest)
    print(f"unknown command: {cmd}", file=sys.stderr)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))