#!/usr/bin/env python3

# SPDX-License-Identifier: MIT
# Copyright © 2025 zfs-rewrite.py Rui Pinheiro

"""Rewrite files in a ZFS dataset exactly once, avoiding hardlink duplicates.

This script walks a given directory tree and runs ``zfs rewrite <file>`` on
each regular file that has not been rewritten yet. To prevent redundant work
on hardlinked files, it tracks files by their device/inode pair and ensures
that only one path for a given hardlinked group is rewritten. It also accepts a
"state file" where already-processed paths are stored and loaded across runs,
making the operation resumable and idempotent.

Key behaviors:

- Deduplication by device/inode: if multiple paths (hardlinks) point to the
  same inode, only the first encountered path is processed.
- Resumable: previously rewritten paths can be supplied via a text file; newly
  rewritten paths are appended to the same file.
- Safety: in ``--dry-run`` mode, the script prints what it would do without
  invoking the ZFS command.

Notes:

- This script must be run with sufficient
  privileges to rewrite the target files.
- The state file records paths, not device/inode pairs. If a file was moved or
  renamed after being rewritten, deduplication is still guaranteed by the
  in-memory device/inode tracking during a single run; however, across runs
  the state file only prevents reprocessing of the recorded paths.

Disclaimer:

- Use at your own risk. This tool can modify on-disk data by issuing
    ``zfs rewrite`` commands. The author provides no warranties and is not
    responsible for any data loss, corruption, downtime, or other damages
    resulting from the use of this script. Always test on non-critical data and
    consider running with ``--dry-run`` first.

Author:

- Rui Pinheiro

License:

- MIT License. See the accompanying ``LICENSE`` file for full text: https://opensource.org/licenses/MIT
"""

import argparse
import math
import os
import subprocess
from typing import Dict, NamedTuple, Set


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for the ZFS rewrite utility.

    Returns:
        argparse.Namespace: Parsed arguments with attributes:
            - path (str): Root directory to recursively process.
            - rewritten_paths_file (str): File recording paths already
              rewritten; appended to as the run proceeds.
            - dry_run (bool): If True, only print actions without executing
              ``zfs rewrite``.
    """
    parser = argparse.ArgumentParser(
        description="Script to rewrite ZFS datasets while avoiding duplicate rewrites of hardlinked files."
    )
    parser.add_argument(
        "-p",
        "--path",
        help="Path to the folder which should be recursively rewritten",
        required=True,
    )
    parser.add_argument(
        "-r",
        "--rewritten-paths-file",
        help="Path to a file with paths that have already been rewritten, and where all rewritten paths will be stored",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        help="Perform a dry run without actually rewriting files",
        action="store_true",
    )
    return parser.parse_args()


FILE_PATH_SEEN: Set[str] = set()
DEVICE_INODES_SEEN: Dict[int, Set[int]] = {}

DevInode = NamedTuple("DevInode", [("dev", int), ("inode", int)])
DevInode.__doc__ = (
    "Device/inode pair uniquely identifying a file within a filesystem.\n\n"
    "Attributes:\n"
    "    dev (int): Device ID (st_dev).\n"
    "    inode (int): Inode number (st_ino)."
)


def check_seen(file_path: str) -> DevInode | None:
    """Check whether a file path or its hardlink group was already processed.

    The function first checks whether the exact ``file_path`` was seen in the
    current process (this run). If not, it inspects the file's stat info and
    checks whether the device/inode pair has been observed before, indicating a
    hardlink to a previously processed file.

    Args:
        file_path (str): Absolute or relative path to a regular file.

    Returns:
        DevInode | None: Returns the file's ``DevInode`` if the path and its
        device/inode have not been seen yet (i.e., should be processed). Returns
        ``None`` if the path or its hardlink group was already seen.

    Raises:
        FileNotFoundError: If the file does not exist.
        PermissionError: If the file cannot be stat'ed due to permissions.
    """
    # Check if this file path has been seen before
    if file_path in FILE_PATH_SEEN:
        return None

    # Check if this file is a hardlink and has been seen before
    stat_info = os.stat(file_path)

    dev_inode_d = DEVICE_INODES_SEEN.get(stat_info.st_dev, None)
    if dev_inode_d is not None:
        if stat_info.st_ino in dev_inode_d:
            return None

    return DevInode(dev=stat_info.st_dev, inode=stat_info.st_ino)


def mark_seen(file_path: str, dev_inode: DevInode | None = None) -> None:
    """Record a file path and its device/inode as processed in this run.

    Args:
        file_path (str): The processed file path to record.
        dev_inode (DevInode | None): Optional precomputed device/inode pair. If
            not provided, the function will ``os.stat`` the path to obtain it.

    Side effects:
        - Updates the in-memory sets ``FILE_PATH_SEEN`` and
          ``DEVICE_INODES_SEEN``.

    Raises:
        FileNotFoundError: If ``dev_inode`` is not provided and the path cannot
            be stat'ed.
        PermissionError: If stat'ing the file is not permitted.
    """
    FILE_PATH_SEEN.add(file_path)

    if dev_inode is None:
        stat_info = os.stat(file_path)
        dev_inode = DevInode(dev=stat_info.st_dev, inode=stat_info.st_ino)

    inodes = DEVICE_INODES_SEEN.get(dev_inode.dev, None)
    if inodes is None:
        inodes = set()
        DEVICE_INODES_SEEN[dev_inode.dev] = inodes
    inodes.add(dev_inode.inode)


def load_rewritten_paths(file_path: str) -> None:
    """Load previously rewritten file paths from a state file.

    Each non-empty line in ``file_path`` is treated as a previously processed
    file path and recorded in the in-memory structures via :func:`mark_seen`.
    Missing state files are ignored.

    Args:
        file_path (str): Path to the state file. If it does not exist, nothing
            is loaded.
    """
    if not os.path.isfile(file_path):
        return

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            path = line.strip()
            if not path:
                continue

            if not os.path.isfile(path):
                continue

            mark_seen(path)


def collect_files(path: str) -> Set[str]:
    """Recursively collect candidate files to rewrite.

    Traverses the directory tree rooted at ``path`` (without following
    symlinks), finds regular files, and filters out those already seen by
    path or by device/inode (hardlink deduplication).

    Args:
        path (str): Root directory to walk.

    Returns:
        Set[str]: A set of file paths that should be considered for rewriting.
        Order is not guaranteed.

    Notes:
        - Non-regular files (directories, FIFOs, device nodes, symlinks) are
          ignored.
    """
    files_set = set()

    for root, _, files in os.walk(path, followlinks=False):
        for name in files:
            file_path = os.path.join(root, name)

            if not os.path.isfile(file_path):
                continue

            if check_seen(file_path) is None:
                continue

            files_set.add(file_path)

    return files_set


def rewrite_zfs_files(
    files: Set[str], rewritten_paths_file: str, dry_run: bool = False
) -> None:
    """Rewrite the provided files using ``zfs rewrite``, with progress output.

    For each file, the function verifies it's a regular file and hasn't been
    processed yet (in this run). If eligible and not a dry run, it invokes the
    ``zfs rewrite <file>`` command. Successfully processed paths are recorded in
    memory and appended to ``rewritten_paths_file``.

    Args:
        files (Set[str]): Set of file paths to process.
        rewritten_paths_file (str): Path to a text file where successfully
            rewritten file paths will be appended (one per line).
        dry_run (bool, optional): If True, only print intended actions without
            executing the ZFS command. Defaults to False.

    Raises:
        subprocess.CalledProcessError: Propagated if the ``zfs`` command fails
            for a file (not raised in dry-run mode).

    Side effects:
        - Writes progress and status messages to stdout.
        - Appends processed file paths to ``rewritten_paths_file`` when not in
          dry-run mode.

    Notes:
        - The state file is opened in append mode, so partially completed runs
          will persist already-processed paths.
        - Progress output includes a running counter and percentage based on
          the total number of candidate files.
    """
    if not dry_run:
        rewritten_f = open(rewritten_paths_file, "a", encoding="utf-8")
    else:
        rewritten_f = None

    try:
        num_files = len(files)
        max_digits = len(str(num_files))

        num_processed = 0
        num_rewritten = 0

        for file_path in files:
            num_processed += 1
            percent = math.floor((num_processed / num_files) * 100)
            progress_str = f"({num_processed:>{max_digits}}/{num_files:>{max_digits}} {percent:>3}%)"

            if not os.path.isfile(file_path):
                print(f"{progress_str} Skipping non-file '{file_path}'")
                continue

            if (dev_inode := check_seen(file_path)) is None:
                print(f"{progress_str} Skipping already rewritten file '{file_path}'")
                continue

            # Rewrite the ZFS file
            if dry_run:
                print(f"{progress_str} Would rewrite ZFS file '{file_path}'")
            else:
                try:
                    print(f"{progress_str} {file_path}")
                    subprocess.check_call(
                        ["zfs", "rewrite", file_path],
                        shell=False,
                    )
                except subprocess.CalledProcessError as e:
                    print(
                        f"ERROR: Failed to rewrite '{file_path}': {e.stderr.decode().strip()}"
                    )
                    raise e

            num_rewritten += 1
            mark_seen(file_path, dev_inode)
            if rewritten_f is not None:
                rewritten_f.write(f"{file_path}\n")

    finally:
        if rewritten_f is not None:
            rewritten_f.close()

    print(f"Done. Processed {num_processed} files, rewritten {num_rewritten} files.")
    if dry_run:
        print("Dry run mode: no files were actually rewritten.")
    assert num_processed == num_files


if __name__ == "__main__":
    args = parse_arguments()
    load_rewritten_paths(args.rewritten_paths_file)
    files = collect_files(args.path)
    rewrite_zfs_files(files, args.rewritten_paths_file, dry_run=args.dry_run)
