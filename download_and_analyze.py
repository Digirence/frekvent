#!/usr/bin/env python3
"""
Download Swedish ebooks and build a word frequency dictionary.
Cross-reference with the Swadesh 207-word list for Swedish.
"""

import os
import re
import time
import json
import requests
from collections import Counter
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BOOKS_DIR = "swedish_books"
OUTPUT_FILE = "swedish_word_frequencies.txt"
SWADESH_OUTPUT = "swadesh_frequency_report.txt"
os.makedirs(BOOKS_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# SWEDISH BOOK SOURCES (Project Gutenberg plain text UTF-8)
# These are public-domain Swedish works available as plain text.
# ---------------------------------------------------------------------------
GUTENBERG_BOOKS = {
    # August Strindberg
    "Röda rummet": 57052,
    "Hemsöborna": 30078,
    "Inferno": 29935,
    "Götiska rummen": 48060,
    "Giftas I": 46012,
    "Giftas II": 46013,
    "Svenska öden och äventyr I": 46096,
    "I havsbandet": 46035,
    "Utopier i verkligheten": 46176,
    "Tjänstekvinnans son I": 46008,
    # Selma Lagerlöf
    "Bannlyst": 39147,
    "Kejsarn av Portugallien": 39087,
    "En herrgårdssägen": 39085,
    "Liljecronas hem": 39086,
    # Viktor Rydberg
    "Singoalla": 28610,
    "Den siste Atenaren I": 10117,
    "Den siste Atenaren II": 10504,
    "Vapensmeden": 11529,
    # Others
    "Folkungaträdet (Heidenstam)": 13371,
    "Det går an (Almqvist)": 14670,
    "Barnen ifrån Frostmofjället": 9828,
    "Fritjofs Saga (Tegnér)": 8518,
    "Gösta Berlings saga I": 28186,
    "Gösta Berlings saga II": 28188,
    "Nils Holgerssons underbara resa I": 36188,
    "Nils Holgerssons underbara resa II": 39772,
    "Pengar (Benedictsson)": 32608,
    "Familjen H*** (Knorring)": 40399,
    "Grannarne (Bremer)": 44099,
    "Hertha (Bremer)": 44098,
    "Drottningens juvelsmycke (Almqvist)": 24232,
    "Karolinerna I (Heidenstam)": 13370,
}

def download_gutenberg_text(book_id, title):
    """Download a Gutenberg book as plain text UTF-8."""
    filename = os.path.join(BOOKS_DIR, f"gutenberg_{book_id}.txt")
    if os.path.exists(filename) and os.path.getsize(filename) > 1000:
        print(f"  [cached] {title}")
        return filename

    # Gutenberg plain text URL patterns
    urls = [
        f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt",
        f"https://www.gutenberg.org/files/{book_id}/{book_id}-0.txt",
        f"https://www.gutenberg.org/ebooks/{book_id}.txt.utf-8",
    ]

    for url in urls:
        try:
            resp = requests.get(url, timeout=30, headers={
                "User-Agent": "SwedishWordFrequency/1.0 (educational project)"
            })
            if resp.status_code == 200 and len(resp.text) > 1000:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"  [downloaded] {title} ({len(resp.text):,} chars)")
                return filename
        except Exception as e:
            continue

    print(f"  [FAILED] {title} (id={book_id})")
    return None


def strip_gutenberg_header_footer(text):
    """Remove Project Gutenberg header and footer boilerplate."""
    # Find start of actual content
    start_markers = [
        "*** START OF THIS PROJECT GUTENBERG",
        "*** START OF THE PROJECT GUTENBERG",
        "***START OF THIS PROJECT GUTENBERG",
        "*** START OF THIS",
        "*END*THE SMALL PRINT",
    ]
    end_markers = [
        "*** END OF THIS PROJECT GUTENBERG",
        "*** END OF THE PROJECT GUTENBERG",
        "***END OF THIS PROJECT GUTENBERG",
        "*** END OF THIS",
        "End of Project Gutenberg",
        "End of the Project Gutenberg",
    ]

    for marker in start_markers:
        idx = text.find(marker)
        if idx != -1:
            nl = text.find("\n", idx)
            if nl != -1:
                text = text[nl + 1:]
            break

    for marker in end_markers:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx]
            break

    return text


# Common English-only words to filter out (not valid Swedish)
ENGLISH_STOPWORDS = {
    "the", "of", "a", "and", "to", "in", "that", "was", "he", "it",
    "his", "is", "with", "for", "as", "had", "her", "not", "but",
    "at", "be", "this", "have", "from", "or", "by", "which", "you",
    "an", "were", "are", "been", "has", "their", "said", "each",
    "she", "do", "its", "about", "would", "them", "made", "after",
    "could", "than", "been", "other", "into", "more", "some", "time",
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
    "including", "limited", "warranties", "merchantability",
    "www", "http", "org", "htm", "txt", "utf",
}

# Words that exist in BOTH Swedish and English — keep these
SHARED_WORDS = {
    "i", "de", "en", "se", "man", "nu", "du", "vi", "den", "han",
    "hon", "sin", "an", "under", "in", "fort", "hand", "modern",
    "barn", "salt", "sand", "berg", "is", "fin", "hall", "full",
    "land", "rum", "folk", "arm", "form", "plan", "film", "ring",
    "start", "all", "organ", "rest", "lever", "horn", "mask",
    "nor", "plan", "order",
}


def extract_words(text):
    """Extract words from text, lowercased, preserving Swedish characters."""
    words = re.findall(r"[a-zåäöéèüæøA-ZÅÄÖÉÈÜÆØ]+", text.lower())
    # Filter out very short or very long tokens, and English-only stopwords
    return [
        w for w in words
        if 1 <= len(w) <= 40
        and (w not in ENGLISH_STOPWORDS or w in SHARED_WORDS)
    ]


# ---------------------------------------------------------------------------
# SWADESH 207-WORD LIST FOR SWEDISH
# Standard linguistic core vocabulary list
# ---------------------------------------------------------------------------
SWADESH_SWEDISH = {
    # Pronouns
    "jag": "I", "du": "you (singular)", "han": "he", "hon": "she",
    "vi": "we", "ni": "you (plural)", "de": "they", "dem": "them",
    "den": "it/that", "det": "it/that (neuter)", "denna": "this",
    "detta": "this (neuter)", "här": "here", "där": "there",
    "vem": "who", "vad": "what", "var": "where", "när": "when",
    "hur": "how", "inte": "not", "alla": "all", "många": "many",
    "några": "some", "få": "few", "andra": "other", "en": "one/a",
    "ett": "one/a (neuter)", "två": "two", "tre": "three", "fyra": "four",
    "fem": "five", "stor": "big", "lång": "long", "bred": "wide",
    "tjock": "thick", "tung": "heavy", "liten": "small", "kort": "short",
    "smal": "narrow", "tunn": "thin", "kvinna": "woman", "man": "man",
    "människa": "human/person", "barn": "child", "hustru": "wife",
    "make": "husband", "mor": "mother", "far": "father",
    "djur": "animal", "fisk": "fish", "fågel": "bird", "hund": "dog",
    "lus": "louse", "orm": "snake", "mask": "worm", "träd": "tree",
    "skog": "forest", "käpp": "stick", "frukt": "fruit", "frö": "seed",
    "blad": "leaf", "rot": "root", "bark": "bark (of tree)",
    "blomma": "flower", "gräs": "grass", "rep": "rope", "hud": "skin",
    "kött": "meat/flesh", "blod": "blood", "ben": "bone", "fett": "fat",
    "ägg": "egg", "horn": "horn", "svans": "tail", "fjäder": "feather",
    "hår": "hair", "huvud": "head", "öra": "ear", "öga": "eye",
    "näsa": "nose", "mun": "mouth", "tand": "tooth", "tunga": "tongue",
    "nagel": "fingernail", "fot": "foot", "knä": "knee", "hand": "hand",
    "vinge": "wing", "mage": "belly", "inälvor": "guts", "hals": "neck",
    "rygg": "back", "bröst": "breast", "hjärta": "heart",
    "lever": "liver", "dricka": "to drink", "äta": "to eat",
    "bita": "to bite", "se": "to see", "höra": "to hear",
    "veta": "to know", "tänka": "to think", "lukta": "to smell",
    "frukta": "to fear", "sova": "to sleep", "leva": "to live",
    "dö": "to die", "döda": "to kill", "kämpa": "to fight",
    "jaga": "to hunt", "slå": "to hit", "skära": "to cut",
    "dela": "to split", "sticka": "to stab", "klia": "to scratch",
    "gräva": "to dig", "simma": "to swim", "flyga": "to fly",
    "gå": "to walk", "komma": "to come", "ligga": "to lie down",
    "sitta": "to sit", "stå": "to stand", "vända": "to turn",
    "falla": "to fall", "ge": "to give", "hålla": "to hold",
    "klämma": "to squeeze", "gnida": "to rub", "tvätta": "to wash",
    "torka": "to wipe", "dra": "to pull", "trycka": "to push",
    "kasta": "to throw", "binda": "to tie", "sy": "to sew",
    "räkna": "to count", "säga": "to say", "sjunga": "to sing",
    "leka": "to play", "flyta": "to float", "flöda": "to flow",
    "frysa": "to freeze", "svälla": "to swell", "sol": "sun",
    "måne": "moon", "stjärna": "star", "vatten": "water", "regn": "rain",
    "flod": "river", "sjö": "lake", "hav": "sea", "salt": "salt",
    "sten": "stone", "sand": "sand", "stoft": "dust", "jord": "earth",
    "moln": "cloud", "dimma": "fog", "himmel": "sky", "vind": "wind",
    "snö": "snow", "is": "ice", "rök": "smoke", "eld": "fire",
    "aska": "ash", "bränna": "to burn", "väg": "road/path",
    "berg": "mountain", "röd": "red", "grön": "green", "gul": "yellow",
    "vit": "white", "svart": "black", "natt": "night", "dag": "day",
    "år": "year", "varm": "warm", "kall": "cold", "full": "full",
    "ny": "new", "gammal": "old", "god": "good", "dålig": "bad",
    "rutten": "rotten", "smutsig": "dirty", "rak": "straight",
    "rund": "round", "vass": "sharp", "slö": "dull", "slät": "smooth",
    "våt": "wet", "torr": "dry", "rätt": "right/correct",
    "nära": "near", "långt": "far", "höger": "right",
    "vänster": "left", "vid": "at/by", "i": "in", "med": "with",
    "och": "and", "om": "if/about", "för": "for/because",
    "namn": "name", "säga": "to say",
    # Additional high-value forms
    "vara": "to be", "ha": "to have", "bli": "to become",
    "ska": "shall/will", "kan": "can", "måste": "must",
    "ville": "wanted", "skulle": "would", "hade": "had",
    "var": "was/where", "är": "is/am/are", "blev": "became",
    "finns": "exists/there is", "sig": "oneself", "sin": "his/her (own)",
    "sitt": "his/her (own, neuter)", "sina": "his/her (own, plural)",
    "min": "my", "mitt": "my (neuter)", "din": "your",
    "hans": "his", "hennes": "her", "som": "who/which/that",
    "att": "to/that", "av": "of/from", "på": "on/at",
    "till": "to", "från": "from", "ut": "out", "upp": "up",
    "ner": "down", "över": "over", "under": "under",
    "mellan": "between", "efter": "after", "före": "before",
    "genom": "through", "hos": "at (someone's place)",
    "mot": "towards/against", "utan": "without",
    "också": "also", "redan": "already", "bara": "only/just",
    "nog": "enough/probably", "mycket": "much/very",
    "mer": "more", "mest": "most", "sedan": "since/then",
    "nu": "now", "aldrig": "never", "alltid": "always",
    "ja": "yes", "nej": "no",
}


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("SWEDISH EBOOK WORD FREQUENCY ANALYZER")
    print("=" * 60)

    # --- Phase 1: Download books ---
    print(f"\n📥 Downloading {len(GUTENBERG_BOOKS)} Swedish books from Project Gutenberg...\n")
    downloaded_files = []
    for title, book_id in GUTENBERG_BOOKS.items():
        filepath = download_gutenberg_text(book_id, title)
        if filepath:
            downloaded_files.append((title, filepath))
        time.sleep(0.5)  # Be polite to servers

    print(f"\n✅ Successfully downloaded {len(downloaded_files)} / {len(GUTENBERG_BOOKS)} books")

    # --- Phase 2: Extract words ---
    print(f"\n📖 Extracting words from {len(downloaded_files)} books...\n")
    total_counter = Counter()
    book_stats = []

    for title, filepath in downloaded_files:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()

        text = strip_gutenberg_header_footer(text)
        words = extract_words(text)
        total_counter.update(words)
        book_stats.append((title, len(words)))
        print(f"  {title}: {len(words):,} words")

    total_words = sum(total_counter.values())
    unique_words = len(total_counter)
    print(f"\n📊 Total words: {total_words:,}")
    print(f"📊 Unique words: {unique_words:,}")

    # --- Phase 3: Save full frequency dictionary ---
    print(f"\n💾 Saving frequency dictionary to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"# Swedish Word Frequency Dictionary\n")
        f.write(f"# Generated from {len(downloaded_files)} Swedish ebooks\n")
        f.write(f"# Total words: {total_words:,}\n")
        f.write(f"# Unique words: {unique_words:,}\n")
        f.write(f"# Format: word<TAB>frequency\n")
        f.write(f"#\n")
        f.write(f"# Books analyzed:\n")
        for title, wc in book_stats:
            f.write(f"#   - {title} ({wc:,} words)\n")
        f.write(f"#\n")
        f.write(f"# {'='*50}\n\n")

        for word, freq in total_counter.most_common():
            f.write(f"{word}\t{freq}\n")

    print(f"  ✅ Saved {unique_words:,} entries")

    # --- Phase 4: Swadesh cross-reference ---
    print(f"\n🔤 Cross-referencing with Swadesh list ({len(SWADESH_SWEDISH)} entries)...\n")

    swadesh_freqs = []
    for sv_word, en_meaning in SWADESH_SWEDISH.items():
        freq = total_counter.get(sv_word, 0)
        swadesh_freqs.append((sv_word, en_meaning, freq))

    # Sort by frequency descending
    swadesh_freqs.sort(key=lambda x: x[2], reverse=True)

    with open(SWADESH_OUTPUT, "w", encoding="utf-8") as f:
        f.write("# Swadesh Core Vocabulary - Frequency in Swedish Literature\n")
        f.write(f"# Based on {len(downloaded_files)} Swedish ebooks ({total_words:,} total words)\n")
        f.write(f"# Sorted by frequency (most common first)\n")
        f.write(f"#\n")
        f.write(f"# STRATEGY: Learn the top words first — they cover the most text.\n")
        f.write(f"# The top 50 Swadesh words alone will cover a huge chunk of any Swedish text.\n")
        f.write(f"#\n")
        f.write(f"# {'='*70}\n")
        f.write(f"# {'Rank':<6}{'Swedish':<15}{'English':<25}{'Frequency':<12}{'% of text'}\n")
        f.write(f"# {'='*70}\n\n")

        for rank, (sv, en, freq) in enumerate(swadesh_freqs, 1):
            pct = (freq / total_words * 100) if total_words > 0 else 0
            f.write(f"{rank:<6}{sv:<15}{en:<25}{freq:<12}{pct:.4f}%\n")

        # Summary statistics
        f.write(f"\n\n# {'='*70}\n")
        f.write(f"# LEARNING PRIORITY TIERS\n")
        f.write(f"# {'='*70}\n\n")

        # Tier 1: top 50
        f.write("## TIER 1 — Learn First (Top 50 by frequency)\n")
        f.write("## These words appear most often in real Swedish text.\n\n")
        cumulative = 0
        for rank, (sv, en, freq) in enumerate(swadesh_freqs[:50], 1):
            pct = (freq / total_words * 100) if total_words > 0 else 0
            cumulative += pct
            f.write(f"  {rank:>3}. {sv:<15} = {en:<25} ({freq:>8,}x, {pct:.3f}%)\n")
        f.write(f"\n  → These 50 words cover {cumulative:.1f}% of all text!\n")

        # Tier 2: 51-100
        f.write(f"\n## TIER 2 — Learn Next (Rank 51-100)\n\n")
        for rank, (sv, en, freq) in enumerate(swadesh_freqs[50:100], 51):
            pct = (freq / total_words * 100) if total_words > 0 else 0
            cumulative += pct
            f.write(f"  {rank:>3}. {sv:<15} = {en:<25} ({freq:>8,}x, {pct:.3f}%)\n")
        f.write(f"\n  → Top 100 words cover {cumulative:.1f}% of all text!\n")

        # Tier 3: rest
        f.write(f"\n## TIER 3 — Learn Later (Rank 101+)\n\n")
        for rank, (sv, en, freq) in enumerate(swadesh_freqs[100:], 101):
            pct = (freq / total_words * 100) if total_words > 0 else 0
            f.write(f"  {rank:>3}. {sv:<15} = {en:<25} ({freq:>8,}x, {pct:.3f}%)\n")

    print(f"  ✅ Saved Swadesh report to {SWADESH_OUTPUT}")

    # --- Print top 30 Swadesh words to console ---
    print(f"\n{'='*70}")
    print("TOP 30 SWADESH WORDS BY FREQUENCY IN SWEDISH LITERATURE")
    print(f"{'='*70}")
    print(f"{'Rank':<6}{'Swedish':<15}{'English':<25}{'Frequency':<12}")
    print("-" * 58)
    for rank, (sv, en, freq) in enumerate(swadesh_freqs[:30], 1):
        print(f"{rank:<6}{sv:<15}{en:<25}{freq:<12,}")

    # --- Print overall top 30 words ---
    print(f"\n{'='*70}")
    print("TOP 30 MOST COMMON WORDS IN SWEDISH LITERATURE (ALL)")
    print(f"{'='*70}")
    for rank, (word, freq) in enumerate(total_counter.most_common(30), 1):
        en = SWADESH_SWEDISH.get(word, "")
        tag = f" ({en})" if en else ""
        print(f"  {rank:>3}. {word:<15} {freq:>10,}{tag}")

    print(f"\n🎉 Done! Check these files:")
    print(f"   📄 {OUTPUT_FILE} — Full frequency dictionary ({unique_words:,} words)")
    print(f"   📄 {SWADESH_OUTPUT} — Swadesh learning priority guide")


if __name__ == "__main__":
    main()
