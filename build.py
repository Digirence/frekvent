#!/usr/bin/env python3
"""
build.py — Swedish Word Frequency Builder

Drop .txt, .pdf, or .epub files into books/ and run:
    python build.py

Generates index.html with Swadesh vocabulary ranked by real frequency.

Usage:
    python build.py                     # process books/ only
    python build.py --include-legacy    # also process swedish_books/
    python build.py --workers 8         # override CPU count
"""

import argparse
import json
import math
import multiprocessing
import os
import re
import sys
import time
import zipfile
from collections import Counter
from html.parser import HTMLParser

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BOOKS_DIR = "books"
LEGACY_BOOKS_DIR = "swedish_books"
TEMPLATE_FILE = "template.html"
OUTPUT_HTML = "index.html"
OUTPUT_FREQ_TXT = "swedish_word_frequencies.txt"

TIER1_SIZE = 50
TIER2_SIZE = 100  # words 51-100

# ---------------------------------------------------------------------------
# PDF SUPPORT (optional — graceful fallback)
# ---------------------------------------------------------------------------
try:
    import fitz  # PyMuPDF

    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False

# ---------------------------------------------------------------------------
# GUTENBERG HEADER/FOOTER STRIPPING
# ---------------------------------------------------------------------------
_START_MARKERS = [
    "*** START OF THIS PROJECT GUTENBERG",
    "*** START OF THE PROJECT GUTENBERG",
    "***START OF THIS PROJECT GUTENBERG",
    "*** START OF THIS",
    "*END*THE SMALL PRINT",
]
_END_MARKERS = [
    "*** END OF THIS PROJECT GUTENBERG",
    "*** END OF THE PROJECT GUTENBERG",
    "***END OF THIS PROJECT GUTENBERG",
    "*** END OF THIS",
    "End of Project Gutenberg",
    "End of the Project Gutenberg",
]


def strip_gutenberg_boilerplate(text):
    for marker in _START_MARKERS:
        idx = text.find(marker)
        if idx != -1:
            nl = text.find("\n", idx)
            if nl != -1:
                text = text[nl + 1 :]
            break
    for marker in _END_MARKERS:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx]
            break
    return text


# ---------------------------------------------------------------------------
# ENGLISH NOISE FILTER
# ---------------------------------------------------------------------------
ENGLISH_STOPWORDS = {
    "the", "of", "a", "and", "to", "in", "that", "was", "he", "it",
    "his", "is", "with", "for", "as", "had", "her", "not", "but",
    "at", "be", "this", "have", "from", "or", "by", "which", "you",
    "an", "were", "are", "been", "has", "their", "said", "each",
    "she", "do", "its", "about", "would", "them", "made", "after",
    "could", "than", "other", "into", "more", "some", "time",
    "very", "when", "come", "can", "no", "most", "only", "over",
    "such", "also", "back", "should", "well", "these", "where",
    "just", "we", "what", "your", "out", "if", "will", "up", "my",
    "who", "so", "they", "did", "him", "work", "any", "may", "then",
    "first", "all", "our", "free", "state", "one", "two", "way",
    "project", "gutenberg", "ebook", "license", "electronic",
    "works", "foundation", "terms", "copy", "distributed",
    "redistribution", "agreement", "trademark", "paragraph",
    "donations", "copyright", "permission", "archive", "donation",
    "volunteers", "compliance", "literary", "domain", "public",
    "refund", "replacement", "defect", "disclaimer", "warranties",
    "including", "limited", "merchantability",
    "www", "http", "org", "htm", "txt", "utf",
}

SHARED_WORDS = {
    "i", "de", "en", "se", "man", "nu", "du", "vi", "den", "han",
    "hon", "sin", "an", "under", "in", "fort", "hand", "modern",
    "barn", "salt", "sand", "berg", "is", "fin", "hall", "full",
    "land", "rum", "folk", "arm", "form", "plan", "film", "ring",
    "start", "all", "organ", "rest", "lever", "horn", "mask",
    "nor", "order",
}

# Regex for Swedish words (including å, ä, ö and common Nordic chars)
_WORD_RE = re.compile(r"[a-zåäöéèüæøA-ZÅÄÖÉÈÜÆØ]+")


# ---------------------------------------------------------------------------
# TEXT EXTRACTORS
# ---------------------------------------------------------------------------
def extract_text_txt(filepath):
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    return strip_gutenberg_boilerplate(text)


def extract_text_pdf(filepath):
    if not PDF_SUPPORT:
        return None
    doc = fitz.open(filepath)
    chunks = []
    for page in doc:
        chunks.append(page.get_text())
    doc.close()
    return "\n".join(chunks)


class _StripTagsParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)


def extract_text_epub(filepath):
    chunks = []
    with zipfile.ZipFile(filepath, "r") as z:
        for name in z.namelist():
            if name.lower().endswith((".html", ".xhtml", ".htm")):
                raw = z.read(name).decode("utf-8", errors="replace")
                parser = _StripTagsParser()
                parser.feed(raw)
                chunks.append(" ".join(parser.parts))
    return "\n".join(chunks)


HANDLERS = {
    ".txt": extract_text_txt,
    ".pdf": extract_text_pdf,
    ".epub": extract_text_epub,
}


def extract_text(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    handler = HANDLERS.get(ext)
    if handler is None:
        return None
    try:
        return handler(filepath)
    except Exception as e:
        print(f"  [WARN] {os.path.basename(filepath)}: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# TOKENIZER
# ---------------------------------------------------------------------------
def tokenize_and_count(text):
    words = _WORD_RE.findall(text.lower())
    counter = Counter()
    for w in words:
        if len(w) < 1 or len(w) > 40:
            continue
        if w in ENGLISH_STOPWORDS and w not in SHARED_WORDS:
            continue
        counter[w] += 1
    return counter


# ---------------------------------------------------------------------------
# WORKER (runs in child process)
# ---------------------------------------------------------------------------
def process_single_file(filepath):
    text = extract_text(filepath)
    if not text or len(text) < 100:
        return None
    counter = tokenize_and_count(text)
    return (counter, filepath)


# ---------------------------------------------------------------------------
# FILE DISCOVERY
# ---------------------------------------------------------------------------
def collect_files(books_dir, include_legacy=False):
    files = []
    for root, _dirs, filenames in os.walk(books_dir):
        for name in filenames:
            ext = os.path.splitext(name)[1].lower()
            if ext in HANDLERS:
                files.append(os.path.join(root, name))
    if include_legacy and os.path.isdir(LEGACY_BOOKS_DIR):
        for name in os.listdir(LEGACY_BOOKS_DIR):
            ext = os.path.splitext(name)[1].lower()
            if ext in HANDLERS:
                files.append(os.path.join(LEGACY_BOOKS_DIR, name))
    return sorted(files)


# ---------------------------------------------------------------------------
# PARALLEL AGGREGATION
# ---------------------------------------------------------------------------
def build_frequency_counter(all_files, num_workers):
    total_counter = Counter()
    n_done = 0
    n_failed = 0
    n_total = len(all_files)
    book_stats = []

    if n_total == 0:
        return total_counter, book_stats, 0

    chunksize = max(1, n_total // (num_workers * 4))

    with multiprocessing.Pool(processes=num_workers) as pool:
        for result in pool.imap_unordered(
            process_single_file, all_files, chunksize=chunksize
        ):
            n_done += 1
            if result is None:
                n_failed += 1
            else:
                sub_counter, filepath = result
                word_count = sum(sub_counter.values())
                total_counter.update(sub_counter)
                book_stats.append((os.path.basename(filepath), word_count))

            pct = n_done / n_total * 100
            bar_len = 30
            filled = int(bar_len * n_done / n_total)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(
                f"\r  {bar} {pct:5.1f}%  ({n_done}/{n_total}, {n_failed} failed)",
                end="",
                flush=True,
            )

    print()
    return total_counter, book_stats, n_failed


# ---------------------------------------------------------------------------
# SWADESH LIST (257 entries)
# ---------------------------------------------------------------------------
SWADESH_SWEDISH = {
    "jag": "I", "du": "you (singular)", "han": "he", "hon": "she",
    "vi": "we", "ni": "you (plural)", "de": "they", "dem": "them",
    "den": "it / that", "det": "it (neuter)", "denna": "this",
    "detta": "this (neuter)", "här": "here", "där": "there",
    "vem": "who", "vad": "what", "var": "was / where", "när": "when",
    "hur": "how", "inte": "not", "alla": "all", "många": "many",
    "några": "some", "få": "few / to get", "andra": "other",
    "en": "a / one", "ett": "a / one (neuter)", "två": "two",
    "tre": "three", "fyra": "four", "fem": "five",
    "stor": "big", "lång": "long", "bred": "wide",
    "tjock": "thick", "tung": "heavy", "liten": "small", "kort": "short",
    "smal": "narrow", "tunn": "thin", "kvinna": "woman", "man": "man",
    "människa": "person", "barn": "child", "hustru": "wife",
    "make": "husband", "mor": "mother", "far": "father",
    "djur": "animal", "fisk": "fish", "fågel": "bird", "hund": "dog",
    "lus": "louse", "orm": "snake", "mask": "worm", "träd": "tree",
    "skog": "forest", "käpp": "stick", "frukt": "fruit", "frö": "seed",
    "blad": "leaf", "rot": "root", "bark": "bark (tree)",
    "blomma": "flower", "gräs": "grass", "rep": "rope", "hud": "skin",
    "kött": "meat / flesh", "blod": "blood", "ben": "bone / leg",
    "fett": "fat", "ägg": "egg", "horn": "horn", "svans": "tail",
    "fjäder": "feather", "hår": "hair", "huvud": "head", "öra": "ear",
    "öga": "eye", "näsa": "nose", "mun": "mouth", "tand": "tooth",
    "tunga": "tongue", "nagel": "fingernail", "fot": "foot",
    "knä": "knee", "hand": "hand", "vinge": "wing", "mage": "belly",
    "inälvor": "guts", "hals": "neck", "rygg": "back (body)",
    "bröst": "breast", "hjärta": "heart", "lever": "liver",
    "dricka": "to drink", "äta": "to eat", "bita": "to bite",
    "se": "to see", "höra": "to hear", "veta": "to know",
    "tänka": "to think", "lukta": "to smell", "frukta": "to fear",
    "sova": "to sleep", "leva": "to live", "dö": "to die",
    "döda": "to kill", "kämpa": "to fight", "jaga": "to hunt",
    "slå": "to hit", "skära": "to cut", "dela": "to split",
    "sticka": "to stab", "klia": "to scratch", "gräva": "to dig",
    "simma": "to swim", "flyga": "to fly", "gå": "to walk / go",
    "komma": "to come", "ligga": "to lie down", "sitta": "to sit",
    "stå": "to stand", "vända": "to turn", "falla": "to fall",
    "ge": "to give", "hålla": "to hold", "klämma": "to squeeze",
    "gnida": "to rub", "tvätta": "to wash", "torka": "to wipe / dry",
    "dra": "to pull", "trycka": "to push", "kasta": "to throw",
    "binda": "to tie", "sy": "to sew", "räkna": "to count",
    "säga": "to say", "sjunga": "to sing", "leka": "to play",
    "flyta": "to float", "flöda": "to flow", "frysa": "to freeze",
    "svälla": "to swell", "sol": "sun", "måne": "moon",
    "stjärna": "star", "vatten": "water", "regn": "rain",
    "flod": "river", "sjö": "lake", "hav": "sea", "salt": "salt",
    "sten": "stone", "sand": "sand", "stoft": "dust", "jord": "earth",
    "moln": "cloud", "dimma": "fog", "himmel": "sky", "vind": "wind",
    "snö": "snow", "is": "ice", "rök": "smoke", "eld": "fire",
    "aska": "ash", "bränna": "to burn", "väg": "road / path",
    "berg": "mountain", "röd": "red", "grön": "green", "gul": "yellow",
    "vit": "white", "svart": "black", "natt": "night", "dag": "day",
    "år": "year", "varm": "warm", "kall": "cold", "full": "full",
    "ny": "new", "gammal": "old", "god": "good", "dålig": "bad",
    "rutten": "rotten", "smutsig": "dirty", "rak": "straight",
    "rund": "round", "vass": "sharp", "slö": "dull / lazy",
    "slät": "smooth", "våt": "wet", "torr": "dry",
    "rätt": "right / correct", "nära": "near", "långt": "far (distance)",
    "höger": "right (dir.)", "vänster": "left", "vid": "at / by",
    "i": "in", "med": "with", "och": "and", "om": "if / about",
    "för": "for / because", "namn": "name",
    # Grammar essentials
    "vara": "to be", "ha": "to have", "bli": "to become",
    "ska": "shall / will", "kan": "can", "måste": "must",
    "ville": "wanted", "skulle": "would", "hade": "had",
    "är": "is / am / are", "blev": "became",
    "finns": "exists / there is", "sig": "oneself",
    "sin": "his/her (own)", "sitt": "his/her (own, n.)",
    "sina": "his/her (own, pl.)", "min": "my", "mitt": "my (neuter)",
    "din": "your", "hans": "his", "hennes": "her",
    "som": "who / which / that", "att": "to / that",
    "av": "of / from", "på": "on / at", "till": "to",
    "från": "from", "ut": "out", "upp": "up", "ner": "down",
    "över": "over", "under": "under", "mellan": "between",
    "efter": "after", "före": "before", "genom": "through",
    "hos": "at (someone's)", "mot": "towards / against",
    "utan": "without", "också": "also", "redan": "already",
    "bara": "only / just", "nog": "enough / probably",
    "mycket": "much / very", "mer": "more", "mest": "most",
    "sedan": "since / then", "nu": "now", "aldrig": "never",
    "alltid": "always", "ja": "yes", "nej": "no",
}


def build_swadesh_data(total_counter):
    results = []
    for sv, en in SWADESH_SWEDISH.items():
        freq = total_counter.get(sv, 0)
        results.append({"sv": sv, "en": en, "freq": freq})
    results.sort(key=lambda x: x["freq"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# TIER COVERAGE
# ---------------------------------------------------------------------------
def compute_tier_coverage(swadesh_data, total_words):
    if total_words == 0:
        return {"tier1": 0, "tier2": 0, "tier3": 0}
    t1 = sum(w["freq"] for w in swadesh_data[:TIER1_SIZE]) / total_words * 100
    t2 = sum(w["freq"] for w in swadesh_data[TIER1_SIZE:TIER2_SIZE]) / total_words * 100
    t3 = sum(w["freq"] for w in swadesh_data[TIER2_SIZE:]) / total_words * 100
    return {"tier1": round(t1, 1), "tier2": round(t2, 1), "tier3": round(t3, 1)}


# ---------------------------------------------------------------------------
# HUMAN-READABLE NUMBER
# ---------------------------------------------------------------------------
def format_short(n):
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


# ---------------------------------------------------------------------------
# HTML RENDERER
# ---------------------------------------------------------------------------
def render_html(swadesh_data, total_words, book_count, tier_coverage):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(script_dir, TEMPLATE_FILE)

    if not os.path.exists(template_path):
        print(f"ERROR: {TEMPLATE_FILE} not found at {template_path}", file=sys.stderr)
        sys.exit(1)

    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    # Build the WORDS JSON array
    words_json = json.dumps(swadesh_data, ensure_ascii=False, separators=(",", ":"))
    # Prevent </script> injection
    words_json = words_json.replace("</", "<\\/")

    # Build tier data for the stats chart
    tier_data = [
        {"label": "Tier 1 (1-50)", "pct": tier_coverage["tier1"], "color": "var(--tier1)"},
        {"label": "Tier 2 (51-100)", "pct": tier_coverage["tier2"], "color": "var(--tier2)"},
        {"label": "Tier 3 (101+)", "pct": tier_coverage["tier3"], "color": "var(--tier3)"},
    ]
    tier_json = json.dumps(tier_data, ensure_ascii=False, separators=(",", ":"))

    tier1_pct = f"{tier_coverage['tier1']:.1f}%"
    tier12_pct = f"{tier_coverage['tier1'] + tier_coverage['tier2']:.1f}%"

    replacements = {
        "{{WORDS_DATA}}": words_json,
        "{{TOTAL_WORDS}}": str(total_words),
        "{{TIER_DATA}}": tier_json,
        "{{BOOK_COUNT}}": str(book_count),
        "{{SWADESH_COUNT}}": str(len(swadesh_data)),
        "{{TOTAL_WORDS_SHORT}}": format_short(total_words),
        "{{TOTAL_WORDS_DISPLAY}}": f"{total_words:,}",
        "{{TIER1_PCT}}": tier1_pct,
        "{{TIER12_PCT}}": tier12_pct,
    }

    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)

    return html


# ---------------------------------------------------------------------------
# FREQUENCY TXT OUTPUT
# ---------------------------------------------------------------------------
def write_freq_txt(total_counter, book_stats, total_words):
    unique = len(total_counter)
    with open(OUTPUT_FREQ_TXT, "w", encoding="utf-8") as f:
        f.write(f"# Swedish Word Frequency Dictionary\n")
        f.write(f"# Generated from {len(book_stats)} books\n")
        f.write(f"# Total words: {total_words:,}\n")
        f.write(f"# Unique words: {unique:,}\n")
        f.write(f"# Format: word\\tfrequency\n")
        f.write(f"#\n")
        f.write(f"# Books analyzed:\n")
        for name, wc in sorted(book_stats):
            f.write(f"#   - {name} ({wc:,} words)\n")
        f.write(f"#\n# {'=' * 50}\n\n")
        for word, freq in total_counter.most_common():
            f.write(f"{word}\t{freq}\n")
    return unique


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Build Swedish word frequency index from books"
    )
    parser.add_argument(
        "--include-legacy",
        action="store_true",
        help=f"Also scan {LEGACY_BOOKS_DIR}/ for .txt files",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 4,
        help="Number of parallel workers (default: CPU count)",
    )
    parser.add_argument(
        "--no-freq-txt",
        action="store_true",
        help=f"Skip generating {OUTPUT_FREQ_TXT}",
    )
    args = parser.parse_args()

    t0 = time.time()

    print("=" * 60)
    print("  SWEDISH WORD FREQUENCY BUILDER")
    print("=" * 60)

    # Ensure books/ exists
    os.makedirs(BOOKS_DIR, exist_ok=True)

    # Check for PDF support
    pdf_note = "yes (PyMuPDF)" if PDF_SUPPORT else "no (pip install PyMuPDF to enable)"
    print(f"\n  Formats:  .txt .epub .pdf({pdf_note})")
    print(f"  Workers:  {args.workers}")

    # Collect files
    all_files = collect_files(BOOKS_DIR, include_legacy=args.include_legacy)

    if not all_files:
        print(f"\n  No books found!")
        print(f"  Drop .txt, .pdf, or .epub files into {BOOKS_DIR}/")
        if not args.include_legacy:
            print(f"  Or run with --include-legacy to also scan {LEGACY_BOOKS_DIR}/")
        sys.exit(1)

    # Count by type
    type_counts = {}
    for f in all_files:
        ext = os.path.splitext(f)[1].lower()
        type_counts[ext] = type_counts.get(ext, 0) + 1

    # Warn about PDFs without support
    if ".pdf" in type_counts and not PDF_SUPPORT:
        print(f"\n  WARNING: Found {type_counts['.pdf']} .pdf files but PyMuPDF is not installed.")
        print(f"           Run: pip install PyMuPDF")
        print(f"           PDF files will be skipped.\n")

    type_str = ", ".join(f"{count} {ext}" for ext, count in sorted(type_counts.items()))
    print(f"  Found:    {len(all_files)} files ({type_str})")

    # Process
    print(f"\n  Processing...\n")
    total_counter, book_stats, n_failed = build_frequency_counter(
        all_files, args.workers
    )

    total_words = sum(total_counter.values())
    unique_words = len(total_counter)
    n_books = len(book_stats)

    print(f"\n  Books processed: {n_books}")
    if n_failed:
        print(f"  Failed: {n_failed}")
    print(f"  Total words: {total_words:,}")
    print(f"  Unique words: {unique_words:,}")

    if total_words == 0:
        print("\n  ERROR: No words extracted. Check your book files.")
        sys.exit(1)

    # Swadesh
    swadesh_data = build_swadesh_data(total_counter)
    tier_coverage = compute_tier_coverage(swadesh_data, total_words)

    found = sum(1 for w in swadesh_data if w["freq"] > 0)
    print(f"  Swadesh words found: {found} / {len(swadesh_data)}")
    print(f"  Tier 1 coverage: {tier_coverage['tier1']}%")

    # Generate HTML
    print(f"\n  Generating {OUTPUT_HTML}...")
    html = render_html(swadesh_data, total_words, n_books, tier_coverage)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, OUTPUT_HTML)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Optional: frequency txt
    if not args.no_freq_txt:
        print(f"  Generating {OUTPUT_FREQ_TXT}...")
        write_freq_txt(total_counter, book_stats, total_words)

    elapsed = time.time() - t0
    print(f"\n  Done in {elapsed:.1f}s!")
    print(f"  Open {OUTPUT_HTML} in your browser.")
    print("=" * 60)


if __name__ == "__main__":
    main()
