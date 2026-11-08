# IEEE Computer Society — Calls for Papers Processor

Scrapes the IEEE Computer Society **Calls for Papers** page, turns every call into a CSV record,
diffs the result against the previously stored `cfp.db`, and writes the refreshed database back.
`index.html` is a standalone viewer for that file.

- Source: <https://www.computer.org/publications/author-resources/calls-for-papers>
- Output: `cfp.db` — CSV, comma separated, UTF-8, header row
- Viewer: `index.html` — DataTables UI, no build step

## Quick start

Install [`uv`](https://docs.astral.sh/uv/) and make sure it is on `PATH`, then:

```bash
# Refresh cfp.db with the parser, then serve the viewer at http://127.0.0.1:3000/
./run.sh

# Serve on a different host and/or port (host defaults to 127.0.0.1, port to 3000)
./run.sh 0.0.0.0 8080
```

`run.sh` behaviour:

1. Moves into the project directory (`cd "$(dirname "$0")"`).
2. Reads `HOST` from `$1` (default `127.0.0.1`) and `PORT` from `$2` (default `3000`).
3. Exits with an error if `uv` is not on `PATH`.
4. Runs `uv run parser.py`, which syncs the `.venv` environment from `pyproject.toml`/`uv.lock`
   and refreshes `cfp.db`.
5. After the parser succeeds, starts Python's static HTTP server on `$HOST:$PORT`, serving the
   repository root so `index.html` and `cfp.db` are loaded over HTTP.
6. Uses `set -euo pipefail`; a failing parser exits non-zero before the viewer is served.

Manual equivalent:

```bash
uv sync                 # create .venv and install the locked dependencies
uv run parser.py        # run the parser inside that environment
uv run python -m http.server 3000 --bind 127.0.0.1  # serve the viewer
```

**Python 3.15.2+** is required: `pyproject.toml` sets `requires-python = ">=3.15.2"` and
`.python-version` pins `3.15.2` (`uv` provisions that interpreter automatically). The parser code
itself only needs 3.12+ (`StrEnum`, PEP 701 f-strings), but the project metadata requires 3.15.2.

## What the parser does

| Step | Function | Description |
| --- | --- | --- |
| Fetch | `get_ieee_cs_page()` | GETs the CFP page with browser-like headers, parses it with `lxml`. |
| Parse | `parse_ieee_cs_cfp_information()` | Iterates `div[aria-label="Call For Paper Card"]` cards. |
| Extract | card fields | Type/Name from `p.style-p-sm` (`Journal - IEEE Transactions on …`), title/link from the first `<a>`, summary from `p.line-clamp-3`, deadline from `p.text-slate-600`. |
| Diff | `match_ieee_cs_cfp_information_with_db()` | Compares web records with `cfp.db` using `Type + Name + Title` as the composite key. |
| Write | `update_db_info()` | Rewrites `cfp.db` unless **only** unmodified records exist; rows are sorted by deadline, undated last. |

Deadlines are accepted in `%d %B %Y`, `%B %d, %Y`, `%B %d %Y` and `%d %b %Y`; unmatched or missing
values are stored as an empty field. Non-printable characters in summaries are replaced with spaces.
Relative links are prefixed with `https://www.computer.org`.

### Record statuses

`DbRecordStatus` classifies every composite key:

| Status | Meaning |
| --- | --- |
| `NEW` | Present on the site, not in `cfp.db` |
| `UPDATED` | Same key, different field values |
| `UNMODIFIED` | Same key and identical values |
| `DELETED` | In `cfp.db`, no longer on the site |

The file is written in the order `NEW`, `UPDATED`, `UNMODIFIED`; deleted rows are dropped.
If every key is `UNMODIFIED`, the file is left untouched and the parser prints
`Skipping DB file update as there are only unmodified records`.

## Output schema (`cfp.db`)

| Column | Type | Example |
| --- | --- | --- |
| `Type` | `Magazine`, `Journal`, `Conference`, `Unknown` | `Journal` |
| `Name` | Publication / conference name | `IEEE Transactions on Services Computing` |
| `Title` | Call title (may start with `CLOSED:`) | `Call for Papers: Special Issue on Large Language Models in Service-Oriented Ecosystems Design: Advances and Applications` |
| `Summary` | Cleaned card text, ~1.3–2.6 KB per row | `Important Dates Submission Deadline: …` |
| `Deadline` | `YYYY-MM-DD` or empty | `2026-10-31` |
| `TitleLink` | Absolute URL of the call | `https://www.computer.org/digital-library/…` |
| `ActionsLink` | Same URL (kept for parity) | `https://www.computer.org/digital-library/…` |

The parser prints a warning if `TitleLink` and `ActionsLink` ever diverge.

## Current data snapshot

Measured from `cfp.db` on 2026-10-22 (the file is regenerated on every run, so these numbers move):

- **138 records**, 7 columns, 341,646 bytes, 139 lines including the header.
- Types: **Unknown 62** (no type prefix on the card), **Magazine 40**, **Journal 36**, **Conference 0**.
- **92 distinct `Name` values**; 138 distinct titles.
- **101 records have a deadline**, 37 are undated.
  - 74 deadlines have already passed, **27 are still upcoming** (next: 2026-10-28).
  - Range: 2024-04-15 → 2027-11-01.
- **5 titles start with `CLOSED:`** (Magazine 3, Journal 1, Unknown 1); none have a future deadline.
- Every record has a summary and a link; `TitleLink == ActionsLink` for **138/138** rows.
- Most frequent names: IEEE Security & Privacy (7), Computer (6), IEEE Transactions on Affective
  Computing (5), IEEE Transactions on Services Computing (4), IEEE Pervasive Computing (4),
  IEEE Micro (4), IEEE Computer Graphics and Applications (4).

First upcoming deadlines:

| Deadline | Type | Name | Title |
| --- | --- | --- | --- |
| 2026-10-28 | Magazine | IEEE Pervasive Computing | Call For Papers: Special Issue on Revisiting Mobility |
| 2026-10-31 | Journal | IEEE Transactions on Services Computing | Call for Papers: Special Issue on Large Language Models in Service-Oriented Ecosystems Design: Advances and Applications |
| 2026-11-01 | Magazine | IEEE Security & Privacy | Call For Papers: Special Issue on Real-World Security: Impact Beyond the Publications |
| 2026-11-02 | Magazine | IEEE Computer Graphics and Applications | Call For Papers: Special Issue on Next-Generation XR Displays: Visual Computing from AI Glasses to Immersive Systems |

`cfp.db` is **git-ignored**; only the parser is versioned. Commit or archive the file separately if
you need a historical snapshot.

## Viewer (`index.html`)

A single self-contained page (jQuery 4.0.0 + DataTables 3.1.4 + PapaParse 5.7.0 from CDNs; jQuery
must load before DataTables).

Features:

- **Load data**: drag & drop `cfp.db`, use *Choose file*, or — when served over HTTP(S) — it is
  fetched automatically from the same directory.
- **Table columns**: Type badge, Name, Title, Summary (3-line clamp with *Read more*), Due Date
  (past / due today / in ≤14 days badges) and a *Call ↗* link.
- **Filters**: type dropdown with per-type counts (raw `Unknown` is shown as **Other**), free-text
  search, page length, and a *Hide outdated* checkbox (keeps undated calls).
- **Stats**: loaded entries, upcoming deadlines and source file name.
- **Theme**: Auto / Light / Dark, persisted in `localStorage` under `cfp-theme`.
- Sorting puts undated deadlines last in both directions.

Serve it locally so the automatic `cfp.db` load works:

```bash
./run.sh
# parser refresh finishes, then open http://127.0.0.1:3000/

./run.sh 127.0.0.1 8080
# parser refresh finishes, then open http://127.0.0.1:8080/
```

Opening `index.html` directly from disk still works; you just have to pick the file manually.

## Project layout

| Path | Purpose |
| --- | --- |
| `parser.py` | Scraper, parser, diff engine and CSV writer. |
| `run.sh` | `uv` entry point; runs the parser, then serves the viewer on a configurable host/port (default `127.0.0.1:3000`). |
| `pyproject.toml` | Project metadata and pinned dependencies. |
| `uv.lock` | Locked dependency versions and hashes (generated by `uv`). |
| `index.html` | Standalone viewer. |
| `cfp.db` | Generated dataset (ignored by Git). |
| `.venv/` | uv-managed virtualenv (ignored by Git). |

## Dependencies

| Package | Version | Role |
| --- | --- | --- |
| `beautifulsoup4` | 4.15.0 | HTML parsing |
| `lxml` | 6.1.3 | Parser backend |
| `requests` | 2.34.2 | HTTP client |
| `certifi` | 2026.10.23 | CA certificates for `requests` |
| `charset-normalizer` | 3.5.2 | `requests` transitive dep |
| `idna` | 3.20 | `requests` transitive dep |
| `urllib3` | 2.8.0 | `requests` transitive dep |
| `soupsieve` | 4.0.1 | CSS selectors for BeautifulSoup |
| `typing_extensions` | 4.16.0 | Backports |

Dependencies are declared with exact pins in `pyproject.toml` and locked with hashes in `uv.lock`.
After changing them, run `uv lock` (or `uv sync`) rather than editing `uv.lock` by hand.

## Notes and limitations

- Scraping depends on the current DOM (`aria-label="Call For Paper Card"` and Tailwind class names);
  markup changes on computer.org will silently reduce the number of parsed cards.
- A card without a `Type - Name` prefix becomes `Unknown`; the parser then derives `Name` from the
  text after the first colon in the title. If that fails too, no composite key can be built and the
  record cannot be stored.
- Only the first link on a card is used; `ActionsLink` is not scraped separately.
- The deadline is not part of the composite key, so a changed date marks an existing call as
  `UPDATED` instead of creating a duplicate.
- Exceptions are caught per stage and printed to stdout; a failed fetch or an unexpected markup
  change can leave later stages without input.

## License

See [LICENSE](LICENSE).
