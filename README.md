# Gumshoe

Fetches external content and files it as markdown into an Obsidian vault.

Gumshoe walks a beat: it visits every configured source, discovers what's new,
fetches it, and writes one markdown file per item. It does not summarize,
synthesize, or report — it acquires and files. The archive is the product.

## Sources

- **YouTube channels** — poll the channel Atom feed, fetch English transcripts.
  Rate-limited to 10 caption fetches per hour, spaced 30 seconds apart. No
  backfill; the cursor starts at the most recent video and moves forward.
- **One-off YouTube videos** — paste a URL with `gumshoe add` (or append to
  `~/.config/gumshoe/queue.txt`). Consumed on successful fetch.
- **Email newsletters** — via `gog` (Gmail). Configured by sender and optional
  subject match. Searches both inbox and archive for the day's newsletters.
- **Podcasts** — poll the RSS feed, download the audio enclosure, downsample
  with `ffmpeg`, and transcribe with OpenAI whisper-1 (~$0.36 per hour of
  audio). Capped at 5 episodes per run; only episodes newer than what the
  vault holds are taken, so adding a source never transcribes the back
  catalog. Requires `ffmpeg` on PATH (`brew install ffmpeg`) and
  `OPENAI_API_KEY` in the environment.
- **Blogs** — poll the RSS feed and extract each new post's page as markdown
  (defuddle, falling back to markdownify). Same no-backfill window as
  podcasts.

## Setup

```bash
mkdir -p ~/.config/gumshoe
cat > ~/.config/gumshoe/config.toml <<'EOF'
vault_root = "~/Vaults/Gumshoe"
account = "personal"            # gog account for newsletters
newsletter_window_days = 1

[[youtube]]
name = "All-In Podcast"
channel_id = "UCESLZhusAkFfsNsApnjF_Cg"

[[youtube]]
name = "Lex Fridman"
channel_id = "UCJIfeSCssxSC_Dhc5s7woww"

[[newsletter]]
name = "Example Newsletter"
sender = "newsletter@example.com"
# subject = "Daily"            # optional subject match
# account = "work"          # per-source override (default: global account)

[[newsletter]]
name = "Work Newsletter"
sender = "research@firm.com"
account = "work"            # reads from the work Gmail, not personal

[[podcast]]
name = "Example Podcast"
feed_url = "https://example.com/feed.xml"
# min_duration = 300         # skip episodes shorter than this (seconds)

[[blog]]
name = "Example Blog"
feed_url = "https://example.com/blog/feed/"

# podcast_window_days = 7    # first-scan lookback for podcasts and blogs
EOF
```

## Usage

```bash
gumshoe run                 # scan + fetch: all sources
gumshoe run <source-name>   # one source only
gumshoe scan [<source>]     # discover and queue only; no fetching
gumshoe fetch               # fetch the persisted queue only
gumshoe add <url>           # append a one-off URL to the queue
gumshoe status              # cursors, queued items, last run, failures
gumshoe sample              # print a sample config
gumshoe --help
```

Run via `uv`:

```bash
./gumshoe.py run
# or
uv run gumshoe.py run
```

Single self-contained Python script with a PEP 723 header. No venv, no
install step. Dependencies (`requests`, `youtube-transcript-api`,
`markdownify`) are declared inline and fetched on first run.

## How it works

Each run has three phases, independently invocable:

1. **Scan** — poll YouTube channel feeds, podcast and blog RSS feeds, query
   gog for newsletters, read the one-off URL queue. Candidate items are
   collected.
2. **Prepare** — dedupe (by item ID, and by date+title to catch re-uploads),
   drop anything already held on disk, sort newest-first, persist the queue.
3. **Fetch** — process the queue with rate and cost limits, writing markdown
   files, advancing cursors. Items that don't fit the hourly caption cap or
   the per-run podcast cap stay queued for the next run; failed items stay
   queued for retry.

All web traffic (feeds, watch pages, article pages, audio enclosures) goes
through a single browser-emulating session — real browser headers and a
persistent cookie jar — so hosts see a consistent client.

Content, once fetched, is never re-fetched, moved, or deleted. The stable
item ID (video ID, message ID, episode GUID, post GUID) makes runs
idempotent — an item whose file already exists is skipped at the cost of a
file stat, not a network call.

## Layout

```
~/Vaults/Gumshoe/<source-slug>/<date>-<title-slug>.md   # the archive
~/.config/gumshoe/config.toml                           # sources, settings
~/.config/gumshoe/state.json                             # cursors, failures
~/.config/gumshoe/queue.txt                              # one-off URL queue
~/.config/gumshoe/gumshoe.lock                           # run lock
```

Each markdown file has YAML frontmatter — source, source type, title,
canonical URL, published date, fetched timestamp, stable item ID — and a body
containing the content (transcript, article, or newsletter text). The
frontmatter schema is the contract with the separate reporting tool (sitrep).

## Egress rotation

When YouTube rate-limits an IP, optional `[hooks]` commands rotate the
network egress: gumshoe runs whatever the config names (`engage` before the
run, `blocked` on an IP block, `release` after) and reads the exit code.
`gumshoe sample` shows the shape; a Tailscale exit-node rotator (jaunt) is
one implementation. Without hooks, gumshoe fetches on the direct connection
and defers blocked items.

## Automation

A scheduled launchd job can run gumshoe daily. Manual runs behave
identically. A lock file prevents concurrent runs.