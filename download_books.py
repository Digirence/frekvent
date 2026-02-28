#!/usr/bin/env python3
"""
download_books.py — Download ~2000 Swedish public domain books

Sources:
  1. Project Gutenberg — Swedish .txt files (~200 books)
  2. Internet Archive Litteraturbanken — Swedish classics as .pdf (~618 books)
  3. Internet Archive — Additional Swedish-language texts (~1200+ books)

All downloaded into books/ for processing by build.py.

Usage:
    python download_books.py              # download all ~2000
    python download_books.py --limit 500  # download first 500
    python download_books.py --skip-pdf   # skip PDFs (faster, text only)
"""

import argparse
import json
import os
import re
import sys
import time
import concurrent.futures
import requests

BOOKS_DIR = "books"
DOWNLOAD_LOG = "books/_download_log.json"
USER_AGENT = "LearnSvenska/1.0 (educational project; Swedish word frequency analysis)"
SESSION = None


def get_session():
    global SESSION
    if SESSION is None:
        SESSION = requests.Session()
        SESSION.headers["User-Agent"] = USER_AGENT
    return SESSION


# ---------------------------------------------------------------------------
# LOGGING (track what we've downloaded to enable resume)
# ---------------------------------------------------------------------------
def load_log():
    if os.path.exists(DOWNLOAD_LOG):
        with open(DOWNLOAD_LOG, "r") as f:
            return json.load(f)
    return {"downloaded": [], "failed": []}


def save_log(log):
    with open(DOWNLOAD_LOG, "w") as f:
        json.dump(log, f, indent=2)


# ---------------------------------------------------------------------------
# SOURCE 1: PROJECT GUTENBERG (~200 Swedish books as .txt)
# ---------------------------------------------------------------------------
def fetch_gutenberg_swedish_ids():
    """Scrape the Gutenberg Swedish catalog page for book IDs."""
    print("  Fetching Project Gutenberg Swedish catalog...")
    s = get_session()

    try:
        resp = s.get(
            "https://www.gutenberg.org/browse/languages/sv",
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"    [WARN] Could not fetch Gutenberg catalog: {e}")
        return []

    # Extract ebook IDs from href="/ebooks/NNNNN"
    ids = list(set(re.findall(r'/ebooks/(\d+)', resp.text)))
    ids = [int(i) for i in ids]
    print(f"    Found {len(ids)} Swedish books on Gutenberg")
    return sorted(ids)


def download_gutenberg_book(book_id, books_dir):
    """Download a single Gutenberg book as .txt."""
    filename = os.path.join(books_dir, f"gutenberg_{book_id}.txt")
    if os.path.exists(filename) and os.path.getsize(filename) > 500:
        return filename  # already downloaded

    s = get_session()
    urls = [
        f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt",
        f"https://www.gutenberg.org/files/{book_id}/{book_id}-0.txt",
        f"https://www.gutenberg.org/ebooks/{book_id}.txt.utf-8",
    ]

    for url in urls:
        try:
            resp = s.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 500:
                with open(filename, "wb") as f:
                    f.write(resp.content)
                return filename
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# SOURCE 2: INTERNET ARCHIVE — LITTERATURBANKEN (~618 PDFs)
# ---------------------------------------------------------------------------
def fetch_litteraturbanken_ids():
    """Query Internet Archive for the Litteraturbanken collection."""
    print("  Fetching Litteraturbanken collection from Internet Archive...")
    s = get_session()

    try:
        resp = s.get(
            "https://archive.org/advancedsearch.php",
            params={
                "q": "identifier:arkivkopia.se-littbank*",
                "fl[]": "identifier",
                "rows": "1000",
                "output": "json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        docs = data.get("response", {}).get("docs", [])
        ids = [d["identifier"] for d in docs]
        print(f"    Found {len(ids)} Litteraturbanken items")
        return ids
    except Exception as e:
        print(f"    [WARN] Could not fetch Litteraturbanken: {e}")
        return []


def download_litteraturbanken_pdf(identifier, books_dir):
    """Download a Litteraturbanken PDF from Internet Archive."""
    filename_part = identifier.replace("arkivkopia.se-littbank-", "")
    filepath = os.path.join(books_dir, f"littbank_{filename_part}.pdf")
    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
        return filepath

    url = f"https://archive.org/download/{identifier}/{filename_part}.pdf"
    s = get_session()
    try:
        resp = s.get(url, timeout=120, stream=True)
        if resp.status_code == 200:
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            if os.path.getsize(filepath) > 1000:
                return filepath
            else:
                os.remove(filepath)
    except Exception:
        if os.path.exists(filepath):
            os.remove(filepath)
    return None


# ---------------------------------------------------------------------------
# SOURCE 3: INTERNET ARCHIVE — ADDITIONAL SWEDISH TEXTS
# ---------------------------------------------------------------------------
def fetch_ia_swedish_book_ids(rows=2000):
    """Search Internet Archive for Swedish-language digitized books."""
    print(f"  Searching Internet Archive for Swedish texts (up to {rows})...")
    s = get_session()
    all_ids = []

    # Multiple queries to get diverse results
    queries = [
        "language:Swedish AND mediatype:texts AND format:Text",
        "language:Swedish AND mediatype:texts AND subject:Swedish",
        "language:swe AND mediatype:texts",
        '(language:Swedish OR language:swe) AND mediatype:texts AND format:"DjVuTXT"',
    ]

    seen = set()
    for q in queries:
        try:
            resp = s.get(
                "https://archive.org/advancedsearch.php",
                params={
                    "q": q,
                    "fl[]": "identifier",
                    "rows": str(rows),
                    "output": "json",
                    "sort[]": "downloads desc",
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            docs = data.get("response", {}).get("docs", [])
            for d in docs:
                ident = d["identifier"]
                if ident not in seen and not ident.startswith("arkivkopia.se-littbank"):
                    seen.add(ident)
                    all_ids.append(ident)
        except Exception as e:
            print(f"    [WARN] Query failed: {e}")
            continue

    print(f"    Found {len(all_ids)} additional Swedish texts")
    return all_ids


def download_ia_text(identifier, books_dir):
    """Download text version of an Internet Archive item.
    Tries: _djvu.txt first, then plain .txt, then .pdf as fallback."""
    txt_path = os.path.join(books_dir, f"ia_{identifier}.txt")
    pdf_path = os.path.join(books_dir, f"ia_{identifier}.pdf")

    if os.path.exists(txt_path) and os.path.getsize(txt_path) > 500:
        return txt_path
    if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
        return pdf_path

    s = get_session()

    # Try to get the file list for this item
    try:
        meta_url = f"https://archive.org/metadata/{identifier}/files"
        resp = s.get(meta_url, timeout=30)
        if resp.status_code != 200:
            return None
        files = resp.json().get("result", [])
    except Exception:
        return None

    # Look for text files first (prefer _djvu.txt, then .txt)
    txt_files = []
    pdf_files = []
    for f in files:
        name = f.get("name", "")
        size = int(f.get("size", 0))
        if name.endswith("_djvu.txt") and size > 500:
            txt_files.append((name, size, 1))  # priority 1
        elif name.endswith(".txt") and size > 500 and not name.startswith("__"):
            txt_files.append((name, size, 2))
        elif name.endswith(".pdf") and size > 1000:
            pdf_files.append((name, size))

    # Try text files first
    txt_files.sort(key=lambda x: (x[2], -x[1]))
    for fname, size, _ in txt_files:
        try:
            url = f"https://archive.org/download/{identifier}/{fname}"
            resp = s.get(url, timeout=120, stream=True)
            if resp.status_code == 200:
                with open(txt_path, "wb") as out:
                    for chunk in resp.iter_content(chunk_size=65536):
                        out.write(chunk)
                if os.path.getsize(txt_path) > 500:
                    return txt_path
                else:
                    os.remove(txt_path)
        except Exception:
            if os.path.exists(txt_path):
                os.remove(txt_path)
            continue

    # Fallback to smallest PDF
    if pdf_files:
        pdf_files.sort(key=lambda x: x[1])
        fname = pdf_files[0][0]
        try:
            url = f"https://archive.org/download/{identifier}/{fname}"
            resp = s.get(url, timeout=120, stream=True)
            if resp.status_code == 200:
                with open(pdf_path, "wb") as out:
                    for chunk in resp.iter_content(chunk_size=65536):
                        out.write(chunk)
                if os.path.getsize(pdf_path) > 1000:
                    return pdf_path
                else:
                    os.remove(pdf_path)
        except Exception:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)

    return None


# ---------------------------------------------------------------------------
# PROGRESS DISPLAY
# ---------------------------------------------------------------------------
def print_progress(label, done, total, failed):
    pct = done / total * 100 if total > 0 else 0
    bar_len = 30
    filled = int(bar_len * done / total) if total > 0 else 0
    bar = "█" * filled + "░" * (bar_len - filled)
    sys.stdout.write(
        f"\r    {bar} {pct:5.1f}% ({done}/{total}, {failed} failed)"
    )
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Download ~2000 Swedish books")
    parser.add_argument(
        "--limit", type=int, default=2000,
        help="Max total books to download (default: 2000)",
    )
    parser.add_argument(
        "--skip-pdf", action="store_true",
        help="Skip PDF downloads (faster, text-only)",
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Parallel download workers (default: 5, be polite)",
    )
    args = parser.parse_args()

    os.makedirs(BOOKS_DIR, exist_ok=True)
    log = load_log()
    already = set(log["downloaded"])
    total_downloaded = len(already)
    target = args.limit

    print("=" * 60)
    print("  SWEDISH BOOK DOWNLOADER")
    print("=" * 60)
    print(f"\n  Target: {target} books")
    print(f"  Already downloaded: {total_downloaded}")
    print(f"  Workers: {args.workers}")
    print(f"  Skip PDFs: {args.skip_pdf}")

    if total_downloaded >= target:
        print(f"\n  Already have {total_downloaded} books. Done!")
        return

    # ---- SOURCE 1: Project Gutenberg ----
    print(f"\n  --- Source 1: Project Gutenberg ---")
    gutenberg_ids = fetch_gutenberg_swedish_ids()
    gutenberg_ids = [i for i in gutenberg_ids if f"gutenberg_{i}" not in already]

    if gutenberg_ids and total_downloaded < target:
        remaining = target - total_downloaded
        batch = gutenberg_ids[:remaining]
        done = 0
        failed = 0
        print(f"    Downloading up to {len(batch)} Gutenberg books...")

        for book_id in batch:
            result = download_gutenberg_book(book_id, BOOKS_DIR)
            done += 1
            if result:
                log["downloaded"].append(f"gutenberg_{book_id}")
                total_downloaded += 1
            else:
                failed += 1
                log["failed"].append(f"gutenberg_{book_id}")
            print_progress("Gutenberg", done, len(batch), failed)
            time.sleep(0.3)

        print()
        save_log(log)
        print(f"    Gutenberg done. Total so far: {total_downloaded}")

    # ---- SOURCE 2: Litteraturbanken (PDFs) ----
    if not args.skip_pdf and total_downloaded < target:
        print(f"\n  --- Source 2: Litteraturbanken (PDFs) ---")
        littbank_ids = fetch_litteraturbanken_ids()
        littbank_ids = [i for i in littbank_ids if f"littbank_{i}" not in already]

        if littbank_ids:
            remaining = target - total_downloaded
            batch = littbank_ids[:remaining]
            done = 0
            failed = 0
            print(f"    Downloading up to {len(batch)} Litteraturbanken PDFs...")

            for ident in batch:
                result = download_litteraturbanken_pdf(ident, BOOKS_DIR)
                done += 1
                if result:
                    log["downloaded"].append(f"littbank_{ident}")
                    total_downloaded += 1
                else:
                    failed += 1
                    log["failed"].append(f"littbank_{ident}")
                print_progress("Littbank", done, len(batch), failed)
                time.sleep(0.5)

            print()
            save_log(log)
            print(f"    Litteraturbanken done. Total so far: {total_downloaded}")

    # ---- SOURCE 3: Internet Archive additional ----
    if total_downloaded < target:
        print(f"\n  --- Source 3: Internet Archive (additional Swedish texts) ---")
        ia_ids = fetch_ia_swedish_book_ids(rows=3000)
        ia_ids = [i for i in ia_ids if f"ia_{i}" not in already]

        if ia_ids:
            remaining = target - total_downloaded
            batch = ia_ids[:remaining]
            done = 0
            failed = 0
            print(f"    Downloading up to {len(batch)} IA texts...")

            for ident in batch:
                if total_downloaded >= target:
                    break
                result = download_ia_text(ident, BOOKS_DIR)
                done += 1
                if result:
                    log["downloaded"].append(f"ia_{ident}")
                    total_downloaded += 1
                else:
                    failed += 1
                    log["failed"].append(f"ia_{ident}")
                print_progress("IA", done, len(batch), failed)
                time.sleep(0.5)

            print()
            save_log(log)
            print(f"    IA done. Total so far: {total_downloaded}")

    # ---- SUMMARY ----
    # Count files actually in books/
    txt_count = len([f for f in os.listdir(BOOKS_DIR) if f.endswith(".txt") and not f.startswith("_")])
    pdf_count = len([f for f in os.listdir(BOOKS_DIR) if f.endswith(".pdf")])
    epub_count = len([f for f in os.listdir(BOOKS_DIR) if f.endswith(".epub")])

    total_size = sum(
        os.path.getsize(os.path.join(BOOKS_DIR, f))
        for f in os.listdir(BOOKS_DIR)
        if os.path.isfile(os.path.join(BOOKS_DIR, f)) and not f.startswith("_")
    )

    print(f"\n{'=' * 60}")
    print(f"  DOWNLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Files in books/:")
    print(f"    .txt:  {txt_count}")
    print(f"    .pdf:  {pdf_count}")
    print(f"    .epub: {epub_count}")
    print(f"    Total: {txt_count + pdf_count + epub_count}")
    print(f"    Size:  {total_size / 1024 / 1024:.0f} MB")
    print(f"\n  Next step: python build.py")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
