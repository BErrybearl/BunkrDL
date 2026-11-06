"""Shared Windows-safe filenames and deterministic collision handling."""
import re
from pathlib import Path


def safe_name(name):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f\x7f]', '', name).rstrip('. ')
    # Windows device names remain reserved even with an extension.
    if re.match(r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)', name, re.I):
        name = '_' + name
    return _fit(name) if name else '未命名'


# Leave 40 units/bytes for the owned .part and .json suffixes.
_NAME_UNITS, _NAME_BYTES = 180 - 40, 240 - 40


def _truncate(text, units, octets):
    result = []
    for char in text:
        u, b = len(char.encode('utf-16-le')) // 2, len(char.encode('utf-8'))
        if u > units or b > octets:
            break
        result.append(char)
        units -= u
        octets -= b
    return ''.join(result)


def _fit(name, suffix=""):
    path = Path(name)
    ext = _truncate(path.suffix, _NAME_UNITS - len(suffix) - 3,
                    _NAME_BYTES - len(suffix) - 9).rstrip('. ')
    tail = suffix + ext
    stem = _truncate(path.stem, _NAME_UNITS - len(tail.encode('utf-16-le')) // 2,
                     _NAME_BYTES - len(tail.encode('utf-8'))).rstrip('. ') or '未命名'
    return (stem + tail).rstrip('. ')


def _numbered(name: str, number: int) -> str:
    return _fit(name, f' ({number})')


def unique_names(records) -> dict[str, str]:
    """Map file IDs to distinct names in record order, case-insensitively."""
    records = list(records)
    reserved = {safe_name(record.name).casefold() for record in records}
    used = set()
    names = {}
    for record in records:
        base = safe_name(record.name)
        name = base
        if name.casefold() in used:
            number = 2
            name = _numbered(base, number)
            while name.casefold() in used or name.casefold() in reserved:
                number += 1
                name = _numbered(base, number)
        names[record.file_id] = name
        used.add(name.casefold())
    return names


