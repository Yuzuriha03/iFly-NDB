# iFly NDB Converter

Convert Fenix `nd.db3` navigation data and NAIP airway segments into the iFly
737 MAX supplemental navdata format.

The converter preserves the installed iFly `Permanent` database and layers
new airports, runways, fixes, navaids, and procedures under `Supplemental`.
NAIP airway changes are merged into `Permanent/WPNAVRTE.txt`; its vendor header
is preserved and the original file is saved once as
`WPNAVRTE.txt.ifly-ndb.bak`.

Existing supplemental `WPNAVAPT.txt`, `WPNAVAID.txt`, and `WPNAVFIX.txt` files
are never removed when a source batch contains no corresponding records.

## Requirements

- Rust 1.85 or newer (only needed when building from source)
- A Fenix `nd.db3`
- An iFly `navdata` directory containing `Permanent`
- NAIP `RTE_SEG.csv` for conversion

`--navdata-path` accepts either the `navdata` directory or its `Permanent`
child. The converter reads the Fenix database in immutable/read-only mode.

## Validate first

Use the built-in validator before converting a newly released AIRAC cycle:

```text
ifly_ndb_converter \
  --db-path /path/to/nd.db3 \
  --navdata-path /path/to/navdata/Permanent \
  --validate-only \
  --no-countdown
```

Validation checks:

- required Fenix tables and the AIRAC cycle;
- matching iFly `cycle.json` and `FMC_Ident.txt` metadata;
- required Permanent files and procedure directories;
- CRLF/fixed-width airport, runway, navaid, and fix records;
- airport, runway, navaid/ILS, route, and procedure record counts.

The command fails rather than combining different AIRAC cycles.

## Convert

```text
ifly_ndb_converter \
  --db-path /path/to/nd.db3 \
  --csv-path /path/to/RTE_SEG.csv \
  --navdata-path /path/to/navdata \
  --no-countdown
```

Optional arguments:

- `--route-file`: explicitly select `Permanent/WPNAVRTE.txt`.
- `--start-terminal-id` and `--end-terminal-id`: convert an explicit inclusive
  Terminal ID range. The end requires the start.
- `--skip-layout-update`: do not update MSFS 2020 `layout.json`/`manifest.json`.
- `--no-countdown`: exit immediately after completion.

Without an explicit Terminal range, records after the historical ZYYJ/Q09
boundary are treated as additions. A stock database whose boundary is already
the final row is valid: it produces no terminal overlay and never creates empty
database files.

## Desktop page

Launch the same executable with:

```text
ifly_ndb_converter --gui
```

The desktop page selects the Fenix database, NAIP CSV, and iFly `navdata`
directory; it can also run an explicit Terminal ID range in successive batches.
The page invokes the normal CLI for each batch, so output semantics and
validation are identical to command-line use. A selected altitude correction
file is applied after the final batch, once all requested procedure files exist.

## Altitude corrections

Use `--altitude-overrides corrections.txt` to apply deterministic corrections
after terminal files are converted. Each non-comment line is:

```text
relative_procedure_file|section_header_without_brackets|altitude
SID/ZWTK.sid|DSC3Z.33.1|6000
```

The correction replaces an existing `Altitude=` value in that exact section or
inserts one if the Fenix source omitted it. Keep cycle-specific correction files
outside the repository and select them from the desktop page when needed.

## Current iFly format guarantees

- Text output uses CRLF line endings.
- Empty source batches preserve existing `WPNAVAPT.txt`, `WPNAVAID.txt`, and
  `WPNAVFIX.txt` overlays instead of deleting manual corrections.
- Supplemental `FMC_Ident.txt` uses `config.CycleName` from the selected DB.
- SID, SID-transition, STAR, STAR-transition, approach, and
  approach-transition file families are supported.
- Packed-BCD frequencies, navaid classes, and fixed-width name truncation match
  the current iFly 2607 corpus.
- Files are written through a same-directory swap with rollback on failure.
- The legacy `Data/navdataSupplemental` runtime directory is not deleted.

## Development

```text
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

The package has a reusable library target in `src/lib.rs`; the CLI orchestration
is in `src/main.rs`, conversion modules are under `src/enroute` and
`src/terminal.rs`, and cross-format checks live in `src/validation.rs`.
Continuous integration runs formatting, Clippy, and tests on Linux and Windows.

## License

GPL-3.0-only. See `LICENSE`. Third-party attribution is in
`THIRD_PARTY_NOTICES.md`.
