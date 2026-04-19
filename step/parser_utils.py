"""Reusable normalization helpers extracted from parser_v4 patterns."""

from __future__ import annotations

from typing import Any


_NUMBER_CHAR_MAP = str.maketrans({
    "А": "A", "В": "V", "Е": "E", "К": "K", "М": "M", "Н": "N", "О": "O",
    "Р": "R", "С": "S", "Т": "T", "У": "U", "Х": "X", "Ҳ": "H", "Қ": "Q",
    "Ғ": "G", "Ў": "O", "Ё": "YO", "Й": "Y", "Л": "L", "Д": "D", "Ж": "J",
    "З": "Z", "И": "I", "П": "P", "Ф": "F", "Ч": "CH", "Ш": "SH", "Я": "YA",
    "Ю": "YU", "Ц": "S", "Ь": "", "Ъ": "", "Ы": "I", "Э": "E",
    "а": "A", "в": "V", "е": "E", "к": "K", "м": "M", "н": "N", "о": "O",
    "р": "R", "с": "S", "т": "T", "у": "U", "х": "X", "ҳ": "H", "қ": "Q",
    "ғ": "G", "ў": "O", "ё": "YO", "й": "Y", "л": "L", "д": "D", "ж": "J",
    "з": "Z", "и": "I", "п": "P", "ф": "F", "ч": "CH", "ш": "SH", "я": "YA",
    "ю": "YU", "ц": "S", "ь": "", "ъ": "", "ы": "I", "э": "E",
})

_CYR_TO_LAT = str.maketrans({
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
    "ж": "j", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "x", "ц": "s", "ч": "ch", "ш": "sh", "щ": "sh", "ъ": "",
    "ы": "i", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    "қ": "q", "ғ": "g", "ў": "o", "ҳ": "h",
})


def normalize_number(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    try:
        return str(int(raw))
    except (TypeError, ValueError):
        return raw


def canonical_number_key(value: Any) -> str:
    """Stable cross-script key for SKU/barcode/doc-like values."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = normalize_number(raw)
    normalized = normalized.translate(_NUMBER_CHAR_MAP).upper()
    return "".join(ch for ch in normalized if ch.isalnum())


def normalize_activity_text(text: Any) -> str:
    value = str(text or "").strip().lower()
    if not value:
        return ""

    for ch in ("’", "`", "ʻ", "ʼ", "ʹ", "´", "‘"):
        value = value.replace(ch, "'")

    value = value.translate(_CYR_TO_LAT)
    return "".join(ch for ch in value if ch.isalnum())


def activity_text_matches(target: str, candidate: str) -> bool:
    t = normalize_activity_text(target)
    c = normalize_activity_text(candidate)
    if not t or not c:
        return False
    return t in c or c in t
