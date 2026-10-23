# IEEE Computer Society — Calls for Papers Processor

Scrapes the IEEE Computer Society **Calls for Papers** page, turns every call into a CSV record,
diffs the result against the previously stored `cfp.db`, and writes the refreshed database back.
`index.html` is a standalone viewer for that file.

- Source: <https://www.computer.org/publications/author-resources/calls-for-papers>
- Output: `cfp.db` — CSV, comma separated, UTF-8, header row
- Viewer: `index.html` — DataTables UI, no build step

## Quick start

```bash
# Create/activate the venv, install requirements and run the parser
./run.sh

# Same, with a custom venv directory and/or script
./run.sh myenv parser.py
```

`run.sh` behaviour:

1. Moves into the project directory (`cd "$(dirname "$0")"`).
2. Uses `python` when available, otherwise `python3`.
3. Creates the venv (`parser` by default) and installs `requirements.txt` on first run.
4. Activates the venv, prints the interpreter version, runs the script and deactivates.

Manual equivalent:

```bash
python3 -m venv parser
source parser/bin/activate
pip install -r requirements.txt
python parser.py
```

**Python 3.12+** is required (the parser uses `StrEnum` and PEP 701 f-strings).
The repository pins `.python-version` to `3.15.0`; the current environment runs Python 3.15.0.

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

A single self-contained page (jQuery + DataTables 1.13.4 + PapaParse from CDNs).

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
python3 -m http.server 8000
# open http://localhost:8000/index.html
```

Opening `index.html` directly from disk still works; you just have to pick the file manually.

## Project layout

| Path | Purpose |
| --- | --- |
| `parser.py` | Scraper, parser, diff engine and CSV writer. |
| `run.sh` | Virtualenv bootstrap + runner. |
| `requirements.txt` | Pinned dependencies. |
| `index.html` | Standalone viewer. |
| `cfp.db` | Generated dataset (ignored by Git). |
| `parser/` | Local virtualenv (ignored by Git). |

## Dependencies

| Package | Version | Role |
| --- | --- | --- |
| `beautifulsoup4` | 4.15.0 | HTML parsing |
| `lxml` | 6.1.3 | Parser backend |
| `requests` | 2.34.2 | HTTP client |
| `certifi`, `charset-normalizer`, `idna`, `urllib3` | — | `requests` transitive deps |
| `soupsieve` | 3.0.4 | CSS selectors for BeautifulSoup |
| `typing_extensions` | 4.16.0 | Backports |

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
