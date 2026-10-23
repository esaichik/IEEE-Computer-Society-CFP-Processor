# AGENTS.md

Instructions and reference data for agents working in this repository.

## 1. What this project is

A small, dependency-light toolchain that turns the IEEE Computer Society **Calls for Papers** page
into a CSV database and displays it in a static web page.

```text
computer.org CFP page
        │  requests + BeautifulSoup
        ▼
   parser.py ──► cfp.db (CSV) ──► index.html (DataTables viewer)
        ▲
        └── diff against the previous cfp.db (NEW / UPDATED / UNMODIFIED / DELETED)
```

- Source URL: <https://www.computer.org/publications/author-resources/calls-for-papers>
- No backend, no package build, no test suite, no CI configuration.
- Target environment: macOS/Linux shell with Bash; the repository is developed on macOS.

## 2. First steps for an agent

1. Read `README.md` for the user-facing description.
2. Run `git status` before editing. Treat modified/untracked files as user work.
3. Inspect `parser.py`, `run.sh`, `requirements.txt` and the relevant part of `index.html` before
   changing behavior.
4. Prefer the smallest change that solves the task; keep the existing style and structure.
5. Validate with the checks in section 10 and report what was and was not run.

Do **not** run a live scrape or rewrite `cfp.db` unless the task requires it. The parser performs a
network request and overwrites the dataset. If a rewrite is required, back up the file first or
confirm with the user; `cfp.db` is real user data (and is git-ignored, so Git cannot restore it).

## 3. Environment and commands

| Item | Value |
| --- | --- |
| Python | 3.12+ required; `.python-version` pins `3.15.0` |
| Virtualenv | `parser/` by default (git-ignored) |
| Shell entry point | `run.sh` |
| Parser entry point | `parser.py` |
| Dataset | `cfp.db` (git-ignored) |
| Viewer | `index.html` (open directly or serve over HTTP) |

Why Python 3.12+: the parser uses `StrEnum` (3.11+) and PEP 701 nested same-quote f-strings such as
`f"row{"s" if n > 1 else ""}"` (3.12+). Do not lower the requirement without rewriting those.

Run the parser:

```bash
./run.sh                 # venv "parser", script "parser.py"
./run.sh myenv parser.py # custom venv and script
```

`run.sh` behavior:

1. `cd "$(dirname "$0")"` so relative paths are valid.
2. Prefers `python`, falls back to `python3`.
3. Creates the venv and installs `requirements.txt` only when `$VENV_NAME/bin/activate` is missing.
4. Activates the venv, prints the Python version, runs the script, deactivates.
5. Uses `set -euo pipefail`; a failing parser exits non-zero.

Dependencies are installed only on first creation. After editing `requirements.txt`, either recreate
the venv or run `pip install -r requirements.txt` inside it.

Serve the viewer:

```bash
python3 -m http.server 8000
# http://localhost:8000/index.html
```

When served over HTTP(S), `index.html` auto-loads `cfp.db` from the same directory. When opened via
`file://`, the user must drop or select the file.

## 4. Repository layout

| Path | Role |
| --- | --- |
| `parser.py` | Scraper, HTML parser, diff engine, CSV reader/writer. |
| `run.sh` | Venv bootstrap and runner. |
| `requirements.txt` | Pinned Python dependencies. |
| `index.html` | Standalone viewer: markup, CSS and JS in one file. |
| `cfp.db` | Generated CSV dataset (git-ignored). |
| `parser/` | Local virtualenv (git-ignored). |
| `README.md` | User documentation; keep it in sync with behavior changes. |
| `LICENSE` | License text. |
| `.python-version` | `3.15.0`. |
| `.gitignore` | Ignores `**/.DS_Store`, `/parser`, `.idea/`, `cfp.db`, `/__pycache__/`. |

Approximate sizes at the time of writing: `parser.py` 285 lines, `index.html` 1294 lines,
`cfp.db` 138 data rows.

## 5. Parser architecture (`parser.py`)

### Constants and configuration

`METADATA` is the single configuration dictionary:

| Key | Value |
| --- | --- |
| `IEEE_CS_CFP_URL` | The CFP page URL |
| `HEADERS` | Browser-like request headers (`Accept`, `Accept-Language`, `Sec-Fetch-*`, Safari UA) |
| `DB_HEADER` | `Type`, `Name`, `Title`, `Summary`, `Deadline`, `TitleLink`, `ActionsLink` |
| `DB_LOCATION` | `""` (current directory) |
| `DB_FILENAME` | `cfp.db` |
| `DB_FIELDS_DELIMITER` | `,` |
| `DB_ENCODING` | `utf-8` |
| `DB_RECORDS_ORDER` | `NEW`, `UPDATED`, `UNMODIFIED` |

Enums:

- `DbRecordStatus`: `NEW`, `UNMODIFIED`, `UPDATED`, `DELETED`.
- `MediaType`: `Magazine`, `Journal`, `Conference`, `Unknown`; `from_value()` is case-insensitive
  and falls back to `Unknown`.
- `DeserializeValueProcessor`: lambdas used while reading `cfp.db` back into typed values.

### Data flow

1. `get_ieee_cs_page()` → `requests.get` with `METADATA["HEADERS"]`, parsed with the `lxml` backend.
2. `parse_ieee_cs_cfp_information(page)`:
   - Finds `div[aria-label="Call For Paper Card"]` containers.
   - `p.style-p-sm` holds `"<Type> - <Name>"`; the part before the first `-` is the type, the rest
     is the name.
   - The first `<a>` supplies both the title text and the relative link; links are prefixed with
     `https://www.computer.org`.
   - `p.line-clamp-3` supplies the summary; non-printable characters become spaces.
   - `p.text-slate-600` supplies the deadline after removing `Submissions Due:`.
   - Accepted deadline formats: `%d %B %Y`, `%B %d, %Y`, `%B %d %Y`, `%d %b %Y`.
   - If no name was found, `try_extract_name_from_title()` takes the text after the first colon.
   - Returns `dict[composite_key, record_dict]`.
3. `match_ieee_cs_cfp_information_with_db(web_data)`:
   - Composite key is the raw concatenation `Type + Name + Title`, with no separator.
   - If `cfp.db` is missing, every web record is `NEW`.
   - Otherwise reads the CSV with `DictReader` (header row skipped manually) and deserializes
     `Type` and `Deadline` before comparing whole dictionaries.
   - Builds `NEW`, `DELETED`, `UPDATED`, `UNMODIFIED` lists.
4. `update_db_info(results)`:
   - Returns early if `only_unmodified_records()` is true.
   - Writes the header and rows in `DB_RECORDS_ORDER` order.
   - Sorts each status group by deadline ascending, with undated rows last:
     `key=(deadline is None, deadline)`.
   - Formats dated rows with `DEFAULT_MEDIA_DEADLINE_FORMAT` (`%Y-%m-%d`).
   - Deleted rows are not written; they disappear from the file.
5. `main()` prints per-status counts, writes the DB, then prints added/deleted rows with `pprint`.

### Invariants — preserve unless the task explicitly changes them

- CSV column order in `METADATA["DB_HEADER"]` must match the reader/writer and `index.html`.
- Field delimiter is `,` and encoding is UTF-8. Always use the `csv` module; summaries contain
  commas, quotes and long text. Never split rows manually.
- The composite key is `Type + Name + Title` (no delimiter). A change to the key or to title/name
  normalization will reclassify existing rows as deleted/new.
- `Deadline` is stored as `YYYY-MM-DD` or empty. The empty value is the "undated" sentinel in both
  Python and JS.
- `TitleLink` and `ActionsLink` are currently identical; `create_media_data_dict()` warns when they
  differ. Do not silently drop one of the columns.
- Record status order is `NEW`, `UPDATED`, `UNMODIFIED`. Deleted rows are dropped.
- The DB is left untouched when every record is `UNMODIFIED`.
- Exceptions are caught per stage and printed to stdout rather than raised. Note that if parsing
  fails, `main()` receives `None` and will fail later; do not rely on the parser recovering.
- `only_unmodified_records()` returns a truthy expression or `None`, despite its `-> None` hint.
  If you touch it, make the return contract explicit and update callers.

### Extending the parser

- **New field**: append to `DB_HEADER`, populate it in `create_media_data_dict()` and the parse
  step, update `index.html` `COLUMNS`/rendering, and update the schema in `README.md`.
- **New type**: add it to `MediaType` and `from_value()`, add a badge in `index.html` if it should
  be visually distinct, and document it.
- **New date format**: add it to the format loop in `parse_ieee_cs_cfp_information()` and keep
  `DEFAULT_MEDIA_DEADLINE_FORMAT`/JS `isoTime()`/`formatIso()` in agreement.
- **New source**: keep extraction inside the parse function, keep the intermediate record shape
  identical, and preserve the composite-key and status semantics.
- Keep configuration in `METADATA` rather than scattering literals.

## 6. Viewer architecture (`index.html`)

Single self-contained file. No bundler, transpiler, or local asset pipeline.

External CDN dependencies (do not change versions casually):

- jQuery 4.0.0 (must load **before** DataTables)
- DataTables 3.1.4 (CSS + JS; `$.fn.dataTable` only exists if jQuery was already loaded)
- PapaParse 5.7.0

Page structure:

- `<head>` bootstrap script resolves the theme before first paint and writes `data-theme`
  (`light`/`dark`) and `data-theme-pref` (`auto`/`light`/`dark`) on `<html>`.
- Hero with title, theme switch (Auto / Light / Dark) and stats (`Entries`, `Upcoming`, `Source`).
- Dropzone + hidden file input (`accept=".db,.csv,text/csv,text/plain"`).
- Status bar (`role="status"`, `aria-live="polite"`, `data-kind="busy|error"`).
- DataTables card with a type filter and a *Hide outdated* checkbox.
- Footer link back to computer.org.

Data handling:

- `PARSE_OPTS`: `header: true`, `dynamicTyping: true`, `skipEmptyLines: 'greedy'`.
- `ingest()` keeps rows with a non-empty `Title` or `Name`, rebuilds the type filter and stats, and
  clears/re-adds rows.
- Auto-load uses `Papa.parse('cfp.db', { download: true })` only when
  `window.location.protocol` is `http:` or `https:`.
- Rows are considered dated only when `Deadline` matches `^\d{4}-\d{1,2}-\d{1,2}$`.

Rendering and behavior:

- Columns are `Type`, `Name`, `Title`, `Summary`, `Deadline`, `TitleLink`; `ActionsLink` is ignored.
- Default sort is Deadline ascending; page length 25; lengths 10/25/50/100/All.
- Raw `Unknown` and empty types are displayed as **Other**. The type filter maps the display label
  back to one or more raw values using an exact regex.
- Titles starting with `CLOSED:` / `CLOSED -` get a `closed` badge.
- Summaries longer than 220 characters get a *Read more* toggle; CSS clamps to 3 lines until opened.
- Deadline display: `deadline passed`, `due today`, or `in N days` (≤14); undated shows
  `Not specified`. Custom `date-with-null-asc|desc` sorters keep undated rows last.
- The global DataTables search filter implements *Hide outdated*; undated rows always remain.
- All untrusted CSV values pass through `esc()`; URLs pass through `safeUrl()`, which allows only
  `http(s)://`, `/`, `./` and otherwise returns `#`.
- Theme preference is stored in `localStorage` under `cfp-theme`.

Frontend conventions:

- Keep CSS custom properties for both light and dark palettes; when adding colors, define them in
  both `:root` and `:root[data-theme="dark"]`.
- Keep the single-file constraint: inline CSS and JS, CDNs only for the three libraries above.
- Do not add build steps, frameworks, or local font/image dependencies.
- Preserve accessibility: visually hidden labels, focus-visible rings, `aria-live` status,
  keyboard-operable controls, and reduced-motion handling.
- DataTables 3 quirks are intentional and documented in comments (native sort arrows, header is not
  sticky, the search input is patched for placeholder/aria-label, the DOM layout is custom). Its
  CSS/markup uses `.dt-container` plus `.dt-length`, `.dt-search`, `.dt-info`, `.dt-paging` and
  `.dt-paging-button`; the old 1.x names (`dataTables_wrapper`, `paginate_button`, …) no longer
  match. Do not “fix” them without understanding the comments.
- The script order is load-bearing: jQuery first, then DataTables, then PapaParse. Loading
  DataTables first leaves `jQuery.fn.dataTable` undefined and the whole page script aborts, so
  `cfp.db` never loads.
- Keep `index.html` resilient to a missing/empty `cfp.db`: it must show the drop prompt rather than
  throw.

## 7. Current dataset snapshot

Measured from `cfp.db` on **2026-10-22**. The parser regenerates this file, so treat the numbers as
a reference, not a contract.

| Metric | Value |
| --- | --- |
| Data rows | 138 |
| Columns | 7 |
| File size | 341,646 bytes |
| Lines including header | 139 |
| Distinct names | 92 |
| Distinct titles | 138 |
| Rows with a deadline | 101 |
| Undated rows | 37 |
| Past deadlines (as of snapshot) | 74 |
| Upcoming deadlines | 27 |
| Deadline range | 2024-04-15 → 2027-11-01 |
| `CLOSED:` titles | 5 (Magazine 3, Journal 1, Unknown 1) |
| Rows with a summary | 138 |
| Rows with `TitleLink == ActionsLink` | 138 |

Type distribution:

| Type | Count |
| --- | --- |
| Unknown | 62 |
| Magazine | 40 |
| Journal | 36 |
| Conference | 0 |

Most frequent names: IEEE Security & Privacy (7), Computer (6), IEEE Transactions on Affective
Computing (5), IEEE Transactions on Services Computing (4), IEEE Pervasive Computing (4),
IEEE Micro (4), IEEE Computer Graphics and Applications (4).

First upcoming deadlines:

| Deadline | Type | Name | Title |
| --- | --- | --- | --- |
| 2026-10-28 | Magazine | IEEE Pervasive Computing | Call For Papers: Special Issue on Revisiting Mobility |
| 2026-10-31 | Journal | IEEE Transactions on Services Computing | Call for Papers: Special Issue on Large Language Models in Service-Oriented Ecosystems Design: Advances and Applications |
| 2026-11-01 | Magazine | IEEE Security & Privacy | Call For Papers: Special Issue on Real-World Security: Impact Beyond the Publications |
| 2026-11-02 | Magazine | IEEE Computer Graphics and Applications | Call For Papers: Special Issue on Next-Generation XR Displays: Visual Computing from AI Glasses to Immersive Systems |

Summary lengths are approximately 1,290–2,616 characters (average ~2,216). Summaries are often
truncated by the source and can end with `&hellip;`.

## 8. Known limitations and pitfalls

- Scraping depends on the current DOM (`aria-label="Call For Paper Card"` and Tailwind class names).
  Markup changes can silently reduce parsed cards; check the printed counts after a run.
- Cards without a `Type - Name` prefix become `Unknown`; the name is then derived from the first
  colon in the title, e.g. `CLOSED: Call for Papers: …` → `Call for Papers: …`.
- Only the first `<a>` on a card is used; `ActionsLink` is not scraped separately.
- The composite key concatenates values without separators, so contrived collisions are possible.
- Reading skips the header row manually (`next(reader)`), so a missing header row shifts the data.
- Exceptions are swallowed with `print`; there is no logging framework or exit-code propagation
  from the parser stages.
- `main()` assumes both parse and match return dictionaries.
- The parser's request is unauthenticated and subject to the site's availability/rate limits.
- No automated tests, linter, formatter, type checker or CI exists. Do not claim they passed.
- `cfp.db` and `parser/` are git-ignored; `.DS_Store` and `.idea/` are also ignored.
- The README snapshot becomes stale after each parser run. Update it only when the change is
  intentional and the user wants current numbers.

## 9. Code style

Python:

- Standard library plus `beautifulsoup4`, `lxml`, `requests` only.
- Type hints on function signatures; `| None` unions; `StrEnum`/`Enum` for closed sets.
- `snake_case` functions/variables, `UPPER_CASE` constants, `PascalCase` classes.
- Keep configuration in `METADATA`; keep constants near the top.
- Prefer small helper functions (`get_tag_attribute_or_default`, `remove_non_printable_chars`,
  `try_extract_name_from_title`, `create_composite_key`).
- Match the existing 4-space indentation and 120-column-ish line style; there is no formatter
  config, so do not reformat unrelated lines.
- Do not add dependencies for trivial tasks; update `requirements.txt` with exact pins if a new
  dependency is genuinely required.

`index.html`:

- ES5-style browser JS (`var`, function expressions, `'use strict'`) inside an IIFE.
- Keep functions grouped by the existing section comments (helpers, renderers, UI, ingest, wiring).
- Prefer `textContent` for plain text and `esc()` for interpolated HTML.
- Preserve the existing theme token names and DataTables integration patterns.

Documentation:

- `README.md` is the user-facing source of truth. Update it when commands, schema, statuses or
  viewer behavior change.
- Keep this file's invariants and pitfalls current when architecture changes.

## 10. Validation checklist

Use what is appropriate for the change; state explicitly what was not run (especially network or
browser checks).

Syntax and static checks:

```bash
python3 -m py_compile parser.py
bash -n run.sh
```

CSV integrity (does not modify the file):

```bash
python3 - <<'PY'
import csv
rows = list(csv.DictReader(open("cfp.db", encoding="utf-8")))
expected = ["Type", "Name", "Title", "Summary", "Deadline", "TitleLink", "ActionsLink"]
assert rows and list(rows[0]) == expected, list(rows[0])
assert all(r["Name"] and r["Title"] for r in rows)
print(len(rows), "rows OK")
PY
```

Parser smoke test (network + rewrites `cfp.db`; only with permission):

```bash
./run.sh
```

Viewer check:

```bash
python3 -m http.server 8000
```

Then verify in a browser: the table loads `cfp.db`, stats populate, type filter counts are correct,
*Hide outdated* keeps undated rows, summaries expand, links open, and Auto/Light/Dark persist across
reloads. Also test the `file://` path and the empty/missing-file state if the change touches loading.

## 11. Change checklist

Before finishing a task:

- [ ] The change matches the existing architecture and naming.
- [ ] `DB_HEADER`, `METADATA`, parser output, diff key, and `index.html` columns remain consistent.
- [ ] Both light and dark palettes still work for UI changes.
- [ ] Untrusted values are escaped and URLs validated.
- [ ] `README.md`/`AGENTS.md` updated if behavior or setup changed.
- [ ] No generated or ignored files (`cfp.db`, `parser/`, `.DS_Store`, `__pycache__`) are staged.
- [ ] Validation results and any skipped checks are reported to the user.

## 12. Git and safety

- Do not delete or overwrite `cfp.db` unless the task requires it; it is ignored and may be the only
  copy. Back it up before a destructive experiment.
- Do not commit `parser/`, `cfp.db`, `.DS_Store`, `.idea/` or `__pycache__/`.
- Do not rewrite Git history, force-push, or change remotes without explicit instruction.
- Treat unexpected modifications as the user's work; inspect before reverting.
- No secrets are stored in the repository; do not add any.
- The viewer loads third-party scripts from CDNs and the parser makes outbound HTTP requests to
  computer.org. Do not introduce additional network calls without a documented reason.
