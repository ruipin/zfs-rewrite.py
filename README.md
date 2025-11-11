# zfs-rewrite.py

Rewrite files in a ZFS dataset exactly once, avoiding hardlink duplicates.

This tool walks a given directory tree and runs `zfs rewrite <file>` on each
regular file that has not been rewritten yet. To prevent redundant work on
hardlinked files, it tracks files by their device/inode pair and ensures that
only one path for a given hardlinked group is rewritten. It also accepts a
"state file" where already-processed paths are stored and loaded across runs,
making the operation resumable and idempotent.

## Key features

- Deduplication by device/inode: if multiple paths (hardlinks) point to the
  same inode, only the first encountered path is processed.
- Resumable: previously rewritten paths can be supplied via a text file; newly
  rewritten paths are appended to the same file.
- Dry run: in `--dry-run` mode, the script prints what it would do without
  invoking the ZFS command.
- Optional physical rewrite: with `-P`/`--physical-rewrite`, use
  `zfs rewrite -P <file>` to perform a physical rewrite.

## Notes

- This script must be run with sufficient privileges to rewrite the target
  files.
- The state file records paths, not device/inode pairs. If a file was moved or
  renamed after being rewritten, deduplication is still guaranteed by the
  in-memory device/inode tracking during a single run; however, across runs the
  state file only prevents reprocessing of the recorded paths.
 - Physical rewrite (`-P`) requires the pool feature `physical_rewrite` to be
   enabled. Do not use `-P` unless your pool supports it; see [OpenZFS docs](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-rewrite.8.html#physical_rewrite).

## Disclaimer

Use at your own risk. This tool can modify on-disk data by issuing `zfs rewrite`
commands. The author provides no warranties and is not responsible for any data
loss, corruption, downtime, or other damages resulting from the use of this
script. Always test on non-critical data and consider running with `--dry-run`
first.

## Usage

The script requires Python 3.11+ (for `|` union types) and the OpenZFS CLI
available in your environment.

**Basic invocation:**

```bash
./zfs-rewrite.py \
  --path /pool/dataset/path \
  --rewritten-paths-file /path/to/rewritten.txt
```

If the state file (`/path/to/rewritten.txt` in the example above) already
exists, an existing run will be resumed. Otherwise, a fresh run will be started
and an empty state file will be created.

**Physical rewrite (requires `physical_rewrite` pool feature):**

```bash
./zfs-rewrite.py \
  -p /pool/dataset/path \
  -r /path/to/rewritten.txt \
  -P
```

Only use `-P` if your pool supports the feature; otherwise the `zfs rewrite -P`
command will fail.

Notes on the state file:

- It appends successfully rewritten file paths (one per line).
- If a file is moved/renamed, the recorded path may no longer match; however,
  within a single run, hardlink deduplication by device/inode still prevents
  duplicate rewrites for links to the same inode.

## Author

Rui Pinheiro

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE)
file for details. A copy of the license text is also available at the
[Open Source Initiative](https://opensource.org/licenses/MIT).

