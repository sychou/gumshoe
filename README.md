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
EOF
```

## Usage

```bash
gumshoe run                 # build queue + fetch: all sources
gumshoe run <source-name>   # one source only
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

Each run has two phases:

1. **Build the queue** — poll YouTube channel feeds, query gog for today's
   newsletters, read the one-off URL queue. Candidate items are collected.
2. **Fetch the queue** — process items newest-first, applying rate limits,
   writing markdown files, advancing cursors. Items that don't fit the hourly
   cap stay queued for next run; failed items stay queued for retry.

Content, once fetched, is never re-fetched, moved, or deleted. The stable
item ID (video ID, message ID) makes runs idempotent — an item whose file
already exists is skipped at the cost of a file stat, not a network call.

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
containing the content (transcript or newsletter text). The frontmatter
schema is the contract with the separate reporting tool (sitrep).

## Automation

A scheduled launchd job can run gumshoe daily. Manual runs behave
identically. A lock file prevents concurrent runs.