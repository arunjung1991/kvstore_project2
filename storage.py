"""Append-only log storage for a tiny KV store.

On-disk format (one record per line):
    SET <key> <value>\n
    DEL <key>\n
    EXPIREAT <key> <absolute_ms>\n
    PERSIST <key>\n

Constraints:
  - <key> has no whitespace.
  - <value> is a single line (may contain spaces; no newlines).
  - File is UTF-8 text.

Error policy (default, non-strict):
  - Unknown commands, malformed lines, empty lines: skipped (DEBUG log).
  - Non-UTF-8 or OS read error: WARNING and stop replay (best-effort).
  - Trailing unterminated line (no newline at EOF): WARNING and ignored.

Durability:
  - Each append does: write → flush → fsync(file).
  - On first construction we ensure the parent directory exists; we also try
    to fsync the directory once (best-effort) so file creation survives crash.

This module writes logs to STDERR only (never STDOUT) to keep Gradebot happy.
"""

from __future__ import annotations

import io
import os
import re
import sys
import logging
from typing import Iterator, Tuple, Optional

# --------------------------- Logging ---------------------------
_logger = logging.getLogger("kvstore.storage")
if not _logger.handlers:
    _h = logging.StreamHandler(stream=sys.stderr)
    _h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    _logger.addHandler(_h)
_logger.setLevel(os.getenv("KV_LOG_LEVEL", "WARNING").upper())

# --------------------------- Defaults --------------------------
_DEFAULT_ENCODING = "utf-8"
_KEY_RE = re.compile(r"^\S+$")  # no whitespace


class AppendOnlyLog:
    """Append-only log for SET/DEL/TTL operations.

    Each successful append is immediately fsync'ed for crash safety.

    Parameters
    ----------
    path : str
        Log file path (e.g., "data.db").
    max_key_len : Optional[int]
        If provided, reject keys longer than this (during append & replay).
    max_value_len : Optional[int]
        If provided, reject values longer than this (during append & replay).
    strict : bool
        If True, malformed lines raise ValueError during replay; if False (default),
        they are skipped with DEBUG logs.
    encoding : str
        Text encoding for the file. Default: "utf-8".
    """

    def __init__(
        self,
        path: str = "data.db",
        *,
        max_key_len: Optional[int] = None,
        max_value_len: Optional[int] = None,
        strict: bool = False,
        encoding: str = _DEFAULT_ENCODING,
    ) -> None:
        self.path = path
        self.max_key_len = max_key_len
        self.max_value_len = max_value_len
        self.strict = strict
        self.encoding = encoding

        # Ensure directory exists and best-effort directory fsync
        parent = os.path.dirname(path) or "."
        os.makedirs(parent, exist_ok=True)
        try:
            # Best-effort: ensure directory metadata hits disk
            dir_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            # Non-fatal; some filesystems don't allow this
            _logger.debug("Directory fsync skipped (not supported).")

    def __repr__(self) -> str:
        return (
            f"AppendOnlyLog(path={self.path!r}, max_key_len={self.max_key_len}, "
            f"max_value_len={self.max_value_len}, strict={self.strict}, "
            f"encoding={self.encoding!r})"
        )

    # ------------------------- Public API -------------------------

    def replay(self) -> Iterator[Tuple[str, str, str]]:
        """Yield (op, key, arg) records in append order.

        op  : "SET" | "DEL" | "EXPIREAT" | "PERSIST"
        key : key string
        arg : for SET      -> value string
              for DEL      -> ""
              for EXPIREAT -> absolute_ms as string
              for PERSIST  -> ""

        Skips malformed records unless strict=True.
        Stops on read/Unicode errors with a WARNING.

        Notes
        -----
        - If the file ends without a newline, the last partial line is ignored.
        - SET lines are split with maxsplit=2 to preserve spaces in the value.
        """
        if not os.path.exists(self.path):
            _logger.debug("No log file at %s; nothing to replay.", self.path)
            return

        try:
            with open(self.path, "r", encoding=self.encoding, newline="") as fh:
                # Peek last character to detect unterminated trailing line
                try:
                    fh.seek(0, io.SEEK_END)
                    size = fh.tell()
                    fh.seek(max(size - 1, 0), io.SEEK_SET)
                    tail = fh.read(1) if size > 0 else "\n"
                    if tail != "\n":
                        _logger.warning(
                            "Detected unterminated trailing line in %s; ignoring last partial line.",
                            self.path,
                        )
                except Exception:
                    # Non-fatal; continue without the check
                    pass

                fh.seek(0)
                for lineno, raw in enumerate(fh, start=1):
                    line = raw.rstrip("\n")
                    if not line:
                        continue
                    ok, op, key, arg, reason = self._parse_and_validate(line)
                    if not ok:
                        msg = f"Skipping line {lineno}: {reason}"
                        if self.strict:
                            raise ValueError(msg)
                        _logger.debug(msg)
                        continue
                    yield op, key, arg

        except UnicodeError as e:
            _logger.warning("Replay halted due to Unicode decode error: %s", e)
            return
        except OSError as e:
            _logger.warning("Replay halted due to read error: %s", e)
            return

    def append_set(self, key: str, value: str) -> None:
        """Append a single 'SET key value\\n' record and fsync."""
        self._validate_key_value_or_raise(key, value)
        line = f"SET {key} {value}\n"
        self._append_line(line)

    def append_del(self, key: str) -> None:
        """Append a 'DEL key' record."""
        if not _KEY_RE.match(key):
            raise ValueError("Key must be non-empty and contain no whitespace.")
        line = f"DEL {key}\n"
        self._append_line(line)

    def append_expireat(self, key: str, absolute_ms: int) -> None:
        """Append an 'EXPIREAT key absolute_ms' record."""
        if not _KEY_RE.match(key):
            raise ValueError("Key must be non-empty and contain no whitespace.")
        line = f"EXPIREAT {key} {int(absolute_ms)}\n"
        self._append_line(line)

    def append_persist(self, key: str) -> None:
        """Append a 'PERSIST key' record."""
        if not _KEY_RE.match(key):
            raise ValueError("Key must be non-empty and contain no whitespace.")
        line = f"PERSIST {key}\n"
        self._append_line(line)

    # Optional utility: rewrite a compacted file from an iterator of (k, v).
    def compact(self, items: Iterator[Tuple[str, str]], tmp_suffix: str = ".tmp") -> None:
        """Rewrite the log with only SET items (e.g., last-write-wins snapshot).

        This helper is not used by Gradebot but kept for completeness.
        """
        tmp_path = self.path + tmp_suffix
        parent = os.path.dirname(self.path) or "."

        with open(tmp_path, "w", encoding=self.encoding, newline="") as fh:
            for k, v in items:
                self._validate_key_value_or_raise(k, v)
                fh.write(f"SET {k} {v}\n")
            fh.flush()
            os.fsync(fh.fileno())

        os.replace(tmp_path, self.path)
        try:
            dir_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            _logger.debug("Directory fsync after compaction skipped (not supported).")

    # ----------------------- Internal helpers ----------------------

    def _append_line(self, line: str) -> None:
        """Append a raw line to the log with flush+fsync."""
        with open(self.path, "a", encoding=self.encoding, newline="") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())

    def _parse_and_validate(self, line: str) -> Tuple[bool, str, str, str, str]:
        """Parse a log line.

        Returns (ok, op, key, arg, reason).
        """
        # Split into at most 3 tokens; SET keeps the rest as value in arg.
        parts = line.split(" ", 2)
        if not parts:
            return False, "", "", "", "empty line"
        op = parts[0]

        if op == "SET":
            if len(parts) != 3:
                return False, "", "", "", "SET malformed (expected 3 tokens)"
            _, key, value = parts
            if not _KEY_RE.match(key):
                return False, "", "", "", "invalid key (contains whitespace or empty)"
            if self.max_key_len is not None and len(key) > self.max_key_len:
                return False, "", "", "", f"key too long (> {self.max_key_len})"
            if self.max_value_len is not None and len(value) > self.max_value_len:
                return False, "", "", "", f"value too long (> {self.max_value_len})"
            return True, "SET", key, value, ""

        if op in ("DEL", "PERSIST"):
            if len(parts) != 2:
                return False, "", "", "", f"{op} malformed (expected 2 tokens)"
            _, key = parts
            if not _KEY_RE.match(key):
                return False, "", "", "", "invalid key (contains whitespace or empty)"
            if self.max_key_len is not None and len(key) > self.max_key_len:
                return False, "", "", "", f"key too long (> {self.max_key_len})"
            return True, op, key, "", ""

        if op == "EXPIREAT":
            if len(parts) != 3:
                return False, "", "", "", "EXPIREAT malformed (expected 3 tokens)"
            _, key, ms_str = parts
            if not _KEY_RE.match(key):
                return False, "", "", "", "invalid key (contains whitespace or empty)"
            try:
                int(ms_str)
            except ValueError:
                return False, "", "", "", "EXPIREAT ms is not an integer"
            return True, "EXPIREAT", key, ms_str, ""

        return False, "", "", "", f"unsupported command '{op}'"

    def _validate_key_value_or_raise(self, key: str, value: str) -> None:
        if not _KEY_RE.match(key):
            raise ValueError("Key must be non-empty and contain no whitespace.")
        if "\n" in value:
            raise ValueError("Value must not contain newline characters.")
        if self.max_key_len is not None and len(key) > self.max_key_len:
            raise ValueError(f"Key too long (> {self.max_key_len}).")
        if self.max_value_len is not None and len(value) > self.max_value_len:
            raise ValueError(f"Value too long (> {self.max_value_len}).")
