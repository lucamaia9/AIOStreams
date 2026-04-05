"""
Port of Radarr's Parser.cs for movie title parsing.
Exact implementation of Radarr's movie matching logic.

Key classes ported:
- Parser.ParseMovieTitle() - Extract title and year from release names
- Parser.CleanMovieTitle() - Normalize titles for matching
- RomanNumeralParser - Roman/Arabic numeral conversion
"""

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

try:
    from transliterate import translit

    HAS_TRANSLITERATE = True
except ImportError:
    HAS_TRANSLITERATE = False


UMLAUT_MAP = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
    "Ä": "Ae",
    "Ö": "Oe",
    "Ü": "Ue",
}

CYRILLIC_RANGE = (0x0400, 0x04FF)


def has_cyrillic(text: str) -> bool:
    for char in text:
        cp = ord(char)
        if CYRILLIC_RANGE[0] <= cp <= CYRILLIC_RANGE[1]:
            return True
    return False


def transliterate_cyrillic(text: str) -> str:
    if not HAS_TRANSLITERATE or not has_cyrillic(text):
        return text
    try:
        return translit(text, "ru", reversed=True)
    except Exception:
        return text


ARTICLES = {"a", "an", "the", "and", "or", "of", "à"}


def normalize_title_for_matching(title: str) -> str:
    """
    Remove articles and normalize for matching.
    Python port of Radarr's NormalizeRegex behavior.
    """
    if not title:
        return ""

    words = re.split(r"[\W_]+", title.lower())
    filtered = [w for w in words if w and w not in ARTICLES]
    return "".join(filtered)


SIMPLE_TITLE_REGEX = re.compile(
    r"(?:(480|540|576|720|1080|2160)[ip]|[xh][\W_]?26[45]|DD\W?5\W1|[<>?*]|"
    r"848x480|1280x720|1920x1080|3840x2160|4096x2160|(8|10)b(it)?|10-bit)\s*?(?![a-b0-9])",
    re.IGNORECASE,
)

WEBSITE_PREFIX_REGEX = re.compile(
    r"^(?:(?:\[|\()\s*)?(?:www\.)?[-a-z0-9-]{1,256}\."
    r"(?<!Naruto-Kun\.)(?:[a-z]{2,6}\.[a-z]{2,6}|xn--[a-z0-9-]{4,}|[a-z]{2,})\b"
    r"(?:\s*(?:\]|\))|[ -]{2,})[ -]*",
    re.IGNORECASE,
)

WEBSITE_POSTFIX_REGEX = re.compile(
    r"(?:\[\s*)?(?:www\.)?[-a-z0-9-]{1,256}\.(?:xn--[a-z0-9-]{4,}|[a-z]{2,6})\b(?:\s*\])$",
    re.IGNORECASE,
)

CLEAN_QUALITY_BRACKETS_REGEX = re.compile(r"\[[a-z0-9 ._-]+\]$", re.IGNORECASE)

CLEAN_TORRENT_SUFFIX_REGEX = re.compile(
    r"\[(?:ettv|rartv|rarbg|cttv|publichd)\]$", re.IGNORECASE
)

REQUEST_INFO_REGEX = re.compile(r"^(?:\[.+?\])+", re.IGNORECASE)

IMDB_ID_REGEX = re.compile(r"tt\d{7,8}", re.IGNORECASE)

TMDB_ID_REGEX = re.compile(r"tmdb(id)?[-_]?(\d+)", re.IGNORECASE)

AKA_REGEX = re.compile(r"[ ]+(?:AKA|\/)[ ]+", re.IGNORECASE)

BRACKETED_AKA_REGEX = re.compile(r"(.*) \([ ]*AKA[ ]+(.*)\)", re.IGNORECASE)

NORMALIZE_AKA_REGEX = re.compile(r"[ ]+(?:A\.K\.A\.)[ ]+", re.IGNORECASE)

REVERSED_TITLE_REGEX = re.compile(r"(?:^|[-._ ])(p027|p0801)[-._ ]", re.IGNORECASE)

REJECT_HASHED_REGEXES = [
    re.compile(r"^[0-9a-zA-Z]{32}$"),
    re.compile(r"^[a-z0-9]{24}$"),
    re.compile(r"^[A-Z]{11}\d{3}$"),
    re.compile(r"^[a-z]{12}\d{3}$"),
    re.compile(r"^Backup_\d{5,}S\d{2}-\d{2}$"),
    re.compile(r"^123$"),
    re.compile(r"^abc$", re.IGNORECASE),
    re.compile(r"^abc[-_. ]xyz", re.IGNORECASE),
    re.compile(r"^b00bs$", re.IGNORECASE),
]

FILE_EXTENSION_REGEX = re.compile(r"\.[a-z0-9]{1,4}$", re.IGNORECASE)

ROMAN_TO_ARABIC = {
    "i": "1",
    "ii": "2",
    "iii": "3",
    "iv": "4",
    "v": "5",
    "vi": "6",
    "vii": "7",
    "viii": "8",
    "ix": "9",
    "x": "10",
    "xi": "11",
    "xii": "12",
    "xiii": "13",
    "xiv": "14",
    "xv": "15",
    "xvi": "16",
    "xvii": "17",
    "xviii": "18",
    "xix": "19",
    "xx": "20",
    "xxi": "21",
}

ARABIC_TO_ROMAN = {v: k for k, v in ROMAN_TO_ARABIC.items()}

REPORT_MOVIE_TITLE_REGEXES = [
    re.compile(
        r"^(?:\[(?P<subgroup>.+?)\][-_. ]?)?(?P<title>(?![(\[]).+?)?"
        r"(?:(?:[-_\W](?<![)\[!]))*(?P<year>(1(8|9)|20)\d{2}(?!p|i|x|\d+|\]|\W\d+)))+"
        r".*?(?P<hash>\[\w{8}\])?(?:$|\.)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?:\[(?P<subgroup>.+?)\][-_. ]?)(?P<title>(?![(\[]).+?)"
        r"((v)(?:\d{1,2})(?:([-_. ])))(\[.*)?(?:[\[(][^])])?.*?"
        r"(?P<hash>\[\w{8}\])(?:$|\.)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?:\[(?P<subgroup>.+?)\][-_. ]?)(?P<title>(?![(\[]).+?)(\[.*).*?"
        r"(?P<hash>\[\w{8}\])(?:$|\.)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?:\[(?P<subgroup>.+?)\][-_. ]?)(?P<title>(?![(\[]).+)(?:[\[(][^])]).*?"
        r"(?P<hash>\[\w{8}\])(?:$|\.)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<title>(?![(\[]).+?)(?:(?:[-_\W](?<![)\[!]))*"
        r"(?P<year>(1(8|9)|20)\d{2}(?!p|i|(1(8|9)|20)\d{2}|\]|\W(1(8|9)|20)\d{2)))+"
        r"(\W+|_|$)(?!\\)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<title>(?![(\[]).+?)?(?:(?:[-_\W](?<![)\[!]))*"
        r"(?P<year>(1(8|9)|20)\d{2}(?!p|i|\d+|\]|\W\d+)))+(\W+|_|$)(?!\\)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<title>.+?)?(?:(?:[-_\W](?<![)\[!]))*(?P<year>(1(8|9)|20)\d{2}"
        r"(?!p|i|\d+|\]|\W\d+)))+(\W+|_|$)(?!\\)",
        re.IGNORECASE,
    ),
]

REPORT_MOVIE_FOLDER_REGEX = re.compile(
    r"^(?:(?:[-_\W](?<![)!]))*(?P<year>(19|20)\d{2}(?!p|i|\d+|\W\d+)))+(\W+|_|$)"
    r"(?P<title>.+?)?$",
    re.IGNORECASE,
)


@dataclass
class ParsedMovieInfo:
    title: str
    year: Optional[int] = None
    titles: list = field(default_factory=list)
    imdb_id: Optional[str] = None
    tmdb_id: Optional[int] = None
    edition: Optional[str] = None
    release_group: Optional[str] = None
    original_title: Optional[str] = None
    release_title: Optional[str] = None


def remove_file_extension(title: str) -> str:
    return FILE_EXTENSION_REGEX.sub("", title)


def remove_accents(text: str) -> str:
    if not text:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )


def replace_german_umlauts(text: str) -> str:
    result = text
    for umlaut, replacement in UMLAUT_MAP.items():
        result = result.replace(umlaut, replacement)
    return result


def clean_movie_title(title: str) -> str:
    """
    Exact port of Radarr's Parser.CleanMovieTitle()
    Used for matching against the index.
    """
    if not title:
        return ""

    if title.isdigit():
        return title.lower()

    result = replace_german_umlauts(title)
    result = normalize_title_for_matching(result)
    result = remove_accents(result)

    return result


def parse_year_from_string(text: str) -> Optional[int]:
    """Extract a 4-digit year (1800-2099) from string."""
    match = re.search(r"\b((?:18|19|20)\d{2})\b", text)
    if match:
        year = int(match.group(1))
        if 1800 <= year <= 2099:
            return year
    return None


def extract_imdb_id(title: str) -> Optional[str]:
    match = IMDB_ID_REGEX.search(title)
    if match:
        return match.group(0).lower()
    return None


def extract_tmdb_id(title: str) -> Optional[int]:
    match = TMDB_ID_REGEX.search(title)
    if match:
        return int(match.group(2))
    return None


def handle_dotted_title(title: str) -> str:
    """
    Handle dot-separated titles, preserving acronyms.
    Port of Radarr's acronym handling logic.
    """
    if "." not in title:
        return title

    parts = title.split(".")
    result_parts = []
    i = 0

    while i < len(parts):
        part = parts[i]
        next_part = parts[i + 1] if i + 1 < len(parts) else ""

        is_acronym = (
            (len(part) == 1 and part.lower() != "a" and not part.isdigit())
            or (part.lower() == "a" and len(next_part) == 1)
            or (part.lower() == "dr")
        )

        if is_acronym and (i < len(parts) - 1 or len(parts) > 1):
            result_parts.append(part + ".")
        else:
            if result_parts and result_parts[-1].endswith("."):
                result_parts.append(" ")
            result_parts.append(part + " ")

        i += 1

    return "".join(result_parts).strip()


def parse_movie_title(raw_title: str) -> Optional[ParsedMovieInfo]:
    """
    Exact port of Radarr's Parser.ParseMovieTitle()
    Returns parsed title, year, and alternative titles.
    """
    if not raw_title or not raw_title.strip():
        return None

    title = raw_title.strip()

    if not any(c.isalnum() for c in title):
        return None

    for pattern in REJECT_HASHED_REGEXES:
        if pattern.match(title):
            return None

    if REVERSED_TITLE_REGEX.search(title):
        title = title[::-1]

    release_title = remove_file_extension(title)
    release_title = release_title.strip("-_")
    release_title = release_title.replace("【", "[").replace("】", "]")

    simple_title = SIMPLE_TITLE_REGEX.sub("", release_title)
    simple_title = WEBSITE_PREFIX_REGEX.sub("", simple_title)
    simple_title = WEBSITE_POSTFIX_REGEX.sub("", simple_title)
    simple_title = CLEAN_TORRENT_SUFFIX_REGEX.sub("", simple_title)
    simple_title = CLEAN_QUALITY_BRACKETS_REGEX.sub("", simple_title)

    all_regexes = list(REPORT_MOVIE_TITLE_REGEXES) + [REPORT_MOVIE_FOLDER_REGEX]

    for pattern in all_regexes:
        match = pattern.search(simple_title)
        if match:
            result = _parse_match_to_info(match, raw_title, release_title)
            if result:
                imdb = extract_imdb_id(release_title)
                tmdb = extract_tmdb_id(release_title)
                if imdb:
                    result.imdb_id = imdb
                if tmdb:
                    result.tmdb_id = tmdb
                return result

    return None


def _parse_match_to_info(
    match: re.Match, original: str, release_title: str
) -> Optional[ParsedMovieInfo]:
    title_val = match.group("title") if "title" in match.groupdict() else None

    if not title_val or title_val == "(":
        return None

    title_str = title_val.replace("_", " ")
    title_str = NORMALIZE_AKA_REGEX.sub(" AKA ", title_str)
    title_str = REQUEST_INFO_REGEX.sub("", title_str).strip()

    title_str = handle_dotted_title(title_str)
    title_str = " ".join(title_str.split())

    year = None
    if "year" in match.groupdict() and match.group("year"):
        try:
            year = int(match.group("year"))
        except (ValueError, TypeError):
            pass

    titles = [title_str]

    unbracketed = BRACKETED_AKA_REGEX.sub(title_str, r"\1 AKA \2")
    for alt in AKA_REGEX.split(unbracketed):
        alt_clean = alt.strip()
        if alt_clean and alt_clean != title_str:
            titles.append(alt_clean)

    return ParsedMovieInfo(
        title=title_str,
        year=year,
        titles=titles,
        original_title=original,
        release_title=release_title,
    )


def generate_title_variants(clean_title: str) -> set:
    """
    Generate title variants with Roman↔Arabic conversion.
    Port of Radarr's FindByTitleCandidates() logic.
    """
    variants = {clean_title}

    for roman, arabic in ROMAN_TO_ARABIC.items():
        if arabic in clean_title:
            variants.add(clean_title.replace(arabic, roman))
        if roman in clean_title:
            variants.add(clean_title.replace(roman, arabic))

    return variants


def title_similarity(title_a: str, title_b: str) -> float:
    """
    Compute similarity between two titles using word-level Jaccard.
    Port of Radarr's title_similarity logic.
    """
    ta = clean_movie_title(title_a)
    tb = clean_movie_title(title_b)

    if not ta or not tb:
        return 0.0

    if ta == tb:
        return 1.0

    words_a = set(ta.split())
    words_b = set(tb.split())

    if not words_a or not words_b:
        return 0.0

    intersection = len(words_a & words_b)
    union = len(words_a | words_b)

    return intersection / union if union > 0 else 0.0
