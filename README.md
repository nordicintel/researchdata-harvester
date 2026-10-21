# Researchdata harvester

Harvest the current [Researchdata catalog](https://researchdata.se/), its corresponding [Dataportalen](https://www.dataportal.se/) records, and accessible metadata from linked repositories.

TO BE FILLED OUT ONCE THE IMPLEMENTATION THE NEW VERSION IS COMPLETED.

## Automatic Parquet and JSONL splitting before commits

Install the locked dependencies and enable the hook once per checkout:

```sh
uv sync --locked
uv run python -m scripts.split_data_files --install
```

Commit normally. The hook replaces staged `.parquet`, `.jsonl`, `.parquet.gz`
and `.jsonl.gz` files of 100 MB or larger, then stages the replacements
automatically. Smaller files stay unchanged. Part numbers precede the complete
suffix, for example `name_part_1.parquet.gz` or `name_part_2.jsonl`.

Configure `[tool.split-data-files]` in `pyproject.toml`. `max_mb = 100` is the
threshold and final maximum; `target_mb = 50` is only an initial size estimate.
MB means 1,000,000 bytes. The initial part count is the original byte size
divided by the target, rounded up, and rows are divided as evenly as possible.
DuckDB writes internally Zstandard-compressed Parquet parts. JSONL splits at
complete lines and preserves their bytes, order and line endings without
reserializing or validating individual JSON records. Gzip inputs produce
independently readable gzip parts, with deterministic gzip headers. For gzip
files, both the initial estimate and final size checks use compressed bytes;
temporary decompression requires disk space for the uncompressed contents.
Parts may exceed the target but must
remain below the maximum; more parts are attempted only if that maximum is hit.

The hook refuses to overwrite existing part files or split a file whose staged
and working copies differ. Resolve that conflict and commit again. Only the
selected data-file replacements are staged; unrelated edits remain untouched.
All splits are prepared before replacement, and an ordinary preparation or
staging failure leaves the original files and index intact. Other metadata
files that reference filenames, such as manifests, are not rewritten by this hook.

For a manual run or smaller development thresholds:

```sh
uv run python -m scripts.split_data_files --max-mb 1 --target-mb 0.5
```

Git does not enable repository hooks automatically after cloning. Run the
installation command in each checkout, including CI checkouts that should use
the hook. The existing published-data builder remains responsible for producing
consistent filenames and manifests.
