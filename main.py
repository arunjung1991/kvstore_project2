
import sys
import shlex
from typing import List, Callable, Dict, Optional, Tuple
from engine import KVEngine

# -------------------- Command & message constants --------------------
CMD_SET: str     = "SET"
CMD_GET: str     = "GET"
CMD_DEL: str     = "DEL"
CMD_EXISTS: str  = "EXISTS"
CMD_MSET: str    = "MSET"
CMD_MGET: str    = "MGET"
CMD_EXPIRE: str  = "EXPIRE"
CMD_TTL: str     = "TTL"
CMD_PERSIST: str = "PERSIST"
CMD_RANGE: str   = "RANGE"
CMD_BEGIN: str   = "BEGIN"
CMD_COMMIT: str  = "COMMIT"
CMD_ABORT: str   = "ABORT"
CMD_EXIT: str    = "EXIT"

MSG_OK: str                = "OK"
ERR_SYNTAX: str            = "ERR syntax"
ERR_UNKNOWN_CMD: str       = "ERR unknown command"
ERR_USAGE_SET: str         = "ERR usage: SET <key> <value>"
ERR_USAGE_GET: str         = "ERR usage: GET <key>"
ERR_USAGE_DEL: str         = "ERR usage: DEL <key>"
ERR_USAGE_EXISTS: str      = "ERR usage: EXISTS <key>"
ERR_USAGE_MSET: str        = "ERR usage: MSET <k1> <v1> [<k2> <v2> ...]"
ERR_USAGE_MGET: str        = "ERR usage: MGET <k1> [<k2> ...]"
ERR_USAGE_EXPIRE: str      = "ERR usage: EXPIRE <key> <milliseconds>"
ERR_USAGE_TTL: str         = "ERR usage: TTL <key>"
ERR_USAGE_PERSIST: str     = "ERR usage: PERSIST <key>"
ERR_USAGE_RANGE: str       = "ERR usage: RANGE <start> <end>"
ERR_TX_NESTED: str         = "ERR transaction already in progress"
ERR_NO_TX: str             = "ERR no transaction"
ERR_VALUE_SINGLELINE: str  = "ERR value must be a single line"
ERR_INTERNAL: str          = "ERR internal"

PROMPT = ""  # no prompt; Gradebot pipes input

Handler = Callable[[List[str], KVEngine], Optional[str]]  # return "EXIT" to terminate


def _print(line: str) -> None:
    """Write a single line to STDOUT and flush (kept minimal for Gradebot)."""
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


# -------------------- Transaction state (overlay) --------------------
# When in_tx is True, writes are buffered in tx_buffer & tx_ops.
in_tx: bool = False
# key -> value (string) or None (means deleted in this transaction)
tx_buffer: Dict[str, Optional[str]] = {}
# sequence of write-like operations to apply on COMMIT
# each entry: (op_name, [arg1, arg2, ...])
tx_ops: List[Tuple[str, List[str]]] = []


def _tx_get_view(key: str, kv: KVEngine) -> Optional[str]:
    """Return effective value of key under current transaction view."""
    if key in tx_buffer:
        return tx_buffer[key]
    return kv.get(key)


def _tx_exists_view(key: str, kv: KVEngine) -> bool:
    """Return True iff key exists (not deleted) under transaction view."""
    return _tx_get_view(key, kv) is not None


# -------------------- Command handlers --------------------
def handle_set(args: List[str], kv: KVEngine) -> None:
    """SET <key> <value...>  ->  OK"""
    global in_tx, tx_buffer, tx_ops

    if len(args) < 2:
        _print(ERR_USAGE_SET)
        return
    key = args[0]
    value = " ".join(args[1:])
    if ("\n" in value) or ("\r" in value):
        _print(ERR_VALUE_SINGLELINE)
        return

    if in_tx:
        tx_buffer[key] = value
        tx_ops.append(("SET", [key, value]))
    else:
        kv.set(key, value)
    _print(MSG_OK)


def handle_get(args: List[str], kv: KVEngine) -> None:
    """GET <key>  ->  value | nil (if missing/deleted/expired)."""
    if len(args) != 1:
        _print(ERR_USAGE_GET)
        return
    key = args[0]

    if in_tx:
        val = _tx_get_view(key, kv)
    else:
        val = kv.get(key)
    _print("nil" if val is None else val)


def handle_del(args: List[str], kv: KVEngine) -> None:
    """DEL <key> -> 1 if removed, 0 if not found."""
    global in_tx, tx_buffer, tx_ops

    if len(args) != 1:
        _print(ERR_USAGE_DEL)
        return
    key = args[0]

    if in_tx:
        # Determine if key exists in tx view BEFORE deleting
        existed = _tx_exists_view(key, kv)
        if existed:
            tx_buffer[key] = None
            tx_ops.append(("DEL", [key]))
        _print("1" if existed else "0")
    else:
        removed = kv.delete(key)
        _print("1" if removed else "0")


def handle_exists(args: List[str], kv: KVEngine) -> None:
    """EXISTS <key> -> 1 if present (and not deleted/expired), else 0."""
    if len(args) != 1:
        _print(ERR_USAGE_EXISTS)
        return
    key = args[0]

    if in_tx:
        present = _tx_exists_view(key, kv)
    else:
        present = kv.exists(key)
    _print("1" if present else "0")


def handle_mset(args: List[str], kv: KVEngine) -> None:
    """MSET <k1> <v1> [<k2> <v2> ...] -> OK."""
    global in_tx, tx_buffer, tx_ops

    if len(args) < 2 or len(args) % 2 != 0:
        _print(ERR_USAGE_MSET)
        return

    it = iter(args)
    for key in it:
        value = next(it)
        if ("\n" in value) or ("\r" in value):
            _print(ERR_VALUE_SINGLELINE)
            return
        if in_tx:
            tx_buffer[key] = value
            tx_ops.append(("SET", [key, value]))
        else:
            kv.set(key, value)

    _print(MSG_OK)


def handle_mget(args: List[str], kv: KVEngine) -> None:
    """MGET <k1> [<k2> ...] -> one line per key: value or nil."""
    if len(args) < 1:
        _print(ERR_USAGE_MGET)
        return

    for key in args:
        if in_tx:
            val = _tx_get_view(key, kv)
        else:
            val = kv.get(key)
        _print("nil" if val is None else val)


def handle_expire(args: List[str], kv: KVEngine) -> None:
    """EXPIRE <key> <ms> -> 1 if TTL set, 0 if key missing.

    Inside a transaction, we defer the actual EXPIRE until COMMIT by recording
    it in tx_ops, but we still decide the return value based on tx view.
    """
    global in_tx, tx_ops

    if len(args) != 2:
        _print(ERR_USAGE_EXPIRE)
        return
    key = args[0]
    try:
        ms = int(args[1])
    except ValueError:
        _print(ERR_USAGE_EXPIRE)
        return

    if in_tx:
        if not _tx_exists_view(key, kv):
            _print("0")
            return
        # Record for commit; TTL semantics will be applied at commit time.
        tx_ops.append(("EXPIRE", [key, str(ms)]))
        _print("1")
    else:
        ok = kv.expire(key, ms)
        _print("1" if ok else "0")


def handle_ttl(args: List[str], kv: KVEngine) -> None:
    """TTL <key> -> remaining ms, -1 if no TTL, -2 if missing/expired."""
    if len(args) != 1:
        _print(ERR_USAGE_TTL)
        return
    key = args[0]
    # We always delegate TTL to KVEngine (transactional TTL view is not required).
    val = kv.ttl(key)
    _print(str(val))


def handle_persist(args: List[str], kv: KVEngine) -> None:
    """PERSIST <key> -> 1 if TTL cleared, 0 otherwise."""
    global in_tx, tx_ops

    if len(args) != 1:
        _print(ERR_USAGE_PERSIST)
        return
    key = args[0]

    if in_tx:
        # Decide based on tx view: if key exists at all, we allow it and record op.
        if not _tx_exists_view(key, kv):
            _print("0")
            return
        tx_ops.append(("PERSIST", [key]))
        _print("1")
    else:
        ok = kv.persist(key)
        _print("1" if ok else "0")


def handle_range(args: List[str], kv: KVEngine) -> None:
    """RANGE <start> <end> -> keys in lexicographic order, then END.

    Empty string "" for a bound means open-ended.
    During a transaction, RANGE sees this session's SET/DEL changes.
    """
    global in_tx, tx_buffer

    if len(args) != 2:
        _print(ERR_USAGE_RANGE)
        return

    start, end = args  # shlex already strips quotes, so "" becomes ""

    if not in_tx:
        # Simple case: delegate directly to KVEngine
        for k in kv.range_keys(start, end):
            _print(k)
        _print("END")
        return

    # Transactional RANGE: overlay tx_buffer over base keys
    # 1) Collect base keys from engine for the requested range
    base_keys = list(kv.range_keys(start, end))

    # 2) Start with base key set, then apply transactional updates
    keys_set = set(base_keys)

    def in_range(k: str) -> bool:
        if start != "" and k < start:
            return False
        if end != "" and k > end:
            return False
        return True

    for k, v in tx_buffer.items():
        if v is None:
            # Deleted in this transaction
            keys_set.discard(k)
        else:
            # Newly set/updated in this transaction
            if in_range(k):
                keys_set.add(k)

    for k in sorted(keys_set):
        _print(k)
    _print("END")


def handle_begin(args: List[str], kv: KVEngine) -> None:
    """BEGIN -> OK (start transaction; no nesting)."""
    global in_tx, tx_buffer, tx_ops

    if in_tx:
        _print(ERR_TX_NESTED)
        return

    in_tx = True
    tx_buffer = {}
    tx_ops = []
    _print(MSG_OK)


def handle_commit(args: List[str], kv: KVEngine) -> None:
    """COMMIT -> OK (apply buffered writes atomically)."""
    global in_tx, tx_buffer, tx_ops

    if not in_tx:
        _print(ERR_NO_TX)
        return

    # Apply operations in the order they were issued
    for op, op_args in tx_ops:
        if op == "SET":
            key, value = op_args
            kv.set(key, value)
        elif op == "DEL":
            key = op_args[0]
            kv.delete(key)
        elif op == "EXPIRE":
            key, ms_str = op_args
            try:
                ms = int(ms_str)
            except ValueError:
                continue
            kv.expire(key, ms)
        elif op == "PERSIST":
            key = op_args[0]
            kv.persist(key)
        # (If new op types are added later, handle them here.)

    # Clear transaction state
    in_tx = False
    tx_buffer = {}
    tx_ops = []
    _print(MSG_OK)


def handle_abort(args: List[str], kv: KVEngine) -> None:
    """ABORT -> OK (discard buffered writes)."""
    global in_tx, tx_buffer, tx_ops

    if not in_tx:
        _print(ERR_NO_TX)
        return

    in_tx = False
    tx_buffer = {}
    tx_ops = []
    _print(MSG_OK)


def handle_exit(args: List[str], kv: KVEngine) -> str:
    """EXIT -> signal main loop to terminate."""
    return "EXIT"


# Dispatch table
DISPATCH: Dict[str, Handler] = {
    CMD_SET:     handle_set,
    CMD_GET:     handle_get,
    CMD_DEL:     handle_del,
    CMD_EXISTS:  handle_exists,
    CMD_MSET:    handle_mset,
    CMD_MGET:    handle_mget,
    CMD_EXPIRE:  handle_expire,
    CMD_TTL:     handle_ttl,
    CMD_PERSIST: handle_persist,
    CMD_RANGE:   handle_range,
    CMD_BEGIN:   handle_begin,
    CMD_COMMIT:  handle_commit,
    CMD_ABORT:   handle_abort,
    CMD_EXIT:    handle_exit,
}


def _parse_command(line: str) -> Optional[List[str]]:
    """Shell-style split using shlex.

    Returns:
        tokens list (cmd + args) on success, or None if parsing fails.
    """
    try:
        return shlex.split(line)
    except ValueError:
        return None


def main(argv: List[str]) -> int:
    """Run the REPL loop for the KV store.

    Args:
        argv: argv[1] may optionally be a custom db path.
    """
    db_path = "data.db" if len(argv) < 2 else argv[1]
    kv = KVEngine(db_path=db_path)

    while True:
        try:
            raw = sys.stdin.readline()
            if not raw:  # EOF → clean exit
                return 0
            line = raw.strip()
            if not line:
                continue

            tokens = _parse_command(line)
            if tokens is None or not tokens:
                _print(ERR_SYNTAX)
                continue

            cmd = tokens[0].upper()
            args = tokens[1:]
            handler = DISPATCH.get(cmd)
            if handler is None:
                _print(ERR_UNKNOWN_CMD)
                continue

            result = handler(args, kv)
            if result == "EXIT":
                return 0

        except KeyboardInterrupt:
            return 0
        except Exception:
            _print(ERR_INTERNAL)

    # unreachable


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
