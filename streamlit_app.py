#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Form + Scanner)
- Add books manually via form
- Scan barcodes from a photo to auto‑fill metadata (title, author, cover, description)
- Add to Library or Wishlist
- Prevents duplicates (by ISBN or Title+Author)
- ENHANCEMENTS (this build):
    - Redesigned Library and Wishlist tabs to use a responsive cover grid.
    - "Read" status checkmark for completed books in the Library.
    - Sorting options for both collections.
    - Live book count in headers.
    - Mobile-optimized CSS for a denser grid on smaller screens.
"""
from __future__ import annotations

import random
import re
import unicodedata
from difflib import SequenceMatcher
import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
from PIL import Image
from gspread.exceptions import APIError, WorksheetNotFound

# Optional barcode support
try:
    from pyzbar.pyzbar import decode as zbar_decode
except Exception:  # pyzbar/libzbar not available in some envs
    zbar_decode = None

# ---------- CONFIG ----------
DEFAULT_SHEET_ID = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"
SPREADSHEET_ID = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")  # only used if no ID
GOOGLE_BOOKS_KEY = st.secrets.get("google_books_api_key", None)

st.set_page_config(page_title="Misiddons Book Database", layout="wide")

UA = {"User-Agent": "misiddons/1.1"}

# ---------- Google Sheets helpers ----------
@st.cache_resource
def connect_to_gsheets():
    if "gcp_service_account" not in st.secrets:
        st.error("gcp_service_account not found in secrets. Add your service account JSON there.")
        return None
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Failed to authorize Google Sheets: {e}")
        return None

@st.cache_data(ttl=60)
def load_data(worksheet: str) -> pd.DataFrame:
    """Fetch a worksheet into a DataFrame. Falls back to get_all_values()."""
    client_local = connect_to_gsheets()
    if not client_local:
        return pd.DataFrame()
    try:
        ss = client_local.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client_local.open(GOOGLE_SHEET_NAME)
        target = worksheet.strip()
        try:
            ws = ss.worksheet(target)
        except WorksheetNotFound:
            names = [w.title for w in ss.worksheets()]
            norm = {n.strip().casefold(): n for n in names}
            if target.strip().casefold() in norm:
                ws = ss.worksheet(norm[target.strip().casefold()])
            else:
                raise
        # Try fast path first
        try:
            df = pd.DataFrame(ws.get_all_records(empty2zero=False, default_blank=""))
            return df.dropna(how="all")
        except Exception:
            vals = ws.get_all_values()
            if not vals:
                return pd.DataFrame()
            header, *rows = vals
            return pd.DataFrame(rows, columns=header).dropna(how="all")
    except WorksheetNotFound:
        try:
            client = connect_to_gsheets()
            ss = client.open_by_key(SPREADSHEET_ID) if client and SPREADSHEET_ID else None
            tabs = [w.title for w in ss.worksheets()] if ss else []
        except Exception:
            tabs = []
        st.error(f"Worksheet '{worksheet}' not found. Available tabs: {tabs}")
        return pd.DataFrame()
    except APIError as e:
        code = getattr(getattr(e, 'response', None), 'status_code', 'unknown')
        st.error(f"Google Sheets API error while loading '{worksheet}' (HTTP {code}). If 404/403, re-share the sheet with the service account and verify the ID.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error loading '{worksheet}': {type(e).__name__}: {e}")
        return pd.DataFrame()

def _get_ws(tab: str):
    """Return a Worksheet handle. (No caching; gspread objects aren't reliably cacheable.)"""
    client = connect_to_gsheets()
    if not client:
        return None
    ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
    t = tab.strip()
    try:
        return ss.worksheet(t)
    except WorksheetNotFound:
        names = [w.title for w in ss.worksheets()]
        norm = {n.strip().casefold(): n for n in names}
        if t.casefold() in norm:
            return ss.worksheet(norm[t.casefold()])
        raise

# ---------- Sheet write helpers ----------
EXACT_HEADERS = [
    "ISBN", "Title", "Author", "Genre", "Language", "Thumbnail", "Description", "Rating", "PublishedDate", "Date Read"
]

ISO_LANG = {
    "EN":"English","IT":"Italian","ES":"Spanish","DE":"German","FR":"French",
    "PT":"Portuguese","NL":"Dutch","SV":"Swedish","NO":"Norwegian","DA":"Danish",
    "FI":"Finnish","RU":"Russian","PL":"Polish","TR":"Turkish","ZH":"Chinese",
    "JA":"Japanese","KO":"Korean","AR":"Arabic","HE":"Hebrew","HI":"Hindi"
}

# Pretty language for 2- and 3-letter codes
ISO_LANG_2 = ISO_LANG.copy()
ISO_LANG_3 = {
    "ENG":"English","ITA":"Italian","SPA":"Spanish","GER":"German","DEU":"German","FRE":"French","FRA":"French",
    "POR":"Portuguese","NLD":"Dutch","DUT":"Dutch","SWE":"Swedish","NOR":"Norwegian","DAN":"Danish",
    "FIN":"Finnish","RUS":"Russian","POL":"Polish","TUR":"Turkish","ZHO":"Chinese","JPN":"Japanese",
    "KOR":"Korean","ARA":"Arabic","HEB":"Hebrew","HIN":"Hindi"
}

def _pretty_lang(code: str) -> str:
    code = (code or "").strip().upper()
    if not code:
        return ""
    if len(code) <= 3:
        return ISO_LANG_2.get(code, code)
    return ISO_LANG_3.get(code, code)

def normalize_language(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    if len(s) <= 3:
        return ISO_LANG.get(s.upper(), s.upper())
    return s

def _normalize_isbn(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s).replace("'", "") if ch.isdigit())

def keep_primary_author(author: str) -> str:
    s = (author or "").strip()
    if not s:
        return ""
    # If it's clearly a list of multiple people separated by commas (3+ parts), keep the first chunk
    if s.count(',') >= 2:
        return s.split(',')[0].strip()
    # Trim lists joined by ' and ' or ' & '
    if ' and ' in s:
        return s.split(' and ')[0].strip()
    if ' & ' in s:
        return s.split(' & ')[0].strip()
    return s

@st.cache_data(ttl=86400)
def _ol_fetch_json(url: str) -> dict:
    try:
        r = requests.get(url, timeout=12, headers=UA)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return {}

@st.cache_data(ttl=86400)
def get_openlibrary_rating(isbn: str):
    """Return (avg, count) rating for the book's first work on Open Library, if any."""
    try:
        bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json")
        works = bj.get("works") or []
        if not works:
            return None, None
        work_key = works[0].get("key")
        if not work_key:
            return None, None
        rj = _ol_fetch_json(f"https://openlibrary.org{work_key}/ratings.json")
        summary = rj.get("summary", {}) if isinstance(rj, dict) else {}
        avg = summary.get("average")
        count = summary.get("count")
        return (avg, count)
    except Exception:
        return None, None

# ---------- Metadata fetchers (improved) ----------
@st.cache_data(ttl=86400)
def get_book_details_google(isbn: str) -> dict:
    if not isbn:
        return {}
    try:
        params = {"q": f"isbn:{isbn}", "printType": "books", "maxResults": 1}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params=params,
            timeout=12,
            headers=UA,
        )
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            return {}
        info = items[0].get("volumeInfo", {})
        desc = info.get("description") or items[0].get("searchInfo", {}).get("textSnippet", "")
        thumbs = info.get("imageLinks") or {}
        thumb = thumbs.get("thumbnail") or thumbs.get("smallThumbnail") or ""
        if thumb.startswith("http://"):
            thumb = thumb.replace("http://", "https://")
        cats = info.get("categories") or []
        authors = info.get("authors") or []
        author = keep_primary_author(authors[0].strip()) if authors else ""

        return {
            "ISBN": isbn,
            "Title": (info.get("title", "") or "").strip(),
            "Author": author,
            "Genre": ", ".join(cats) if cats else "",
            "Language": (info.get("language") or "").upper(),
            "Thumbnail": thumb,
            "Description": (desc or "").strip(),
            "Rating": str(info.get("averageRating", "")),
            "PublishedDate": info.get("publishedDate", ""),
        }
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def get_book_details_openlibrary(isbn: str) -> dict:
    try:
        # Primary: jscmd=data
        r = requests.get(
            "https://openlibrary.org/api/books",
            params={"bibkeys": f"ISBN:{isbn}", "jscmd": "data", "format": "json"},
            timeout=12,
            headers=UA,
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}") or {}

        # Author(s)
        authors_list = data.get("authors", [])
        author = keep_primary_author(authors_list[0].get("name", "").strip()) if authors_list else ""

        # Subjects -> Genre
        subjects = ", ".join([s.get("name","") for s in data.get("subjects", []) if s])

        # Cover
        cover = (data.get("cover") or {}).get("large") \
             or (data.get("cover") or {}).get("medium") \
             or ""

        # Description (varies across endpoints)
        desc = data.get("description", "")
        if isinstance(desc, dict):
            desc = desc.get("value", "")

        # Fallbacks via /isbn and works endpoint
        bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json") or {}
        if not desc:
            # Try work description
            works = bj.get("works") or []
            if works and works[0].get("key"):
                wk = works[0]["key"]
                wj = _ol_fetch_json(f"https://openlibrary.org{wk}.json") or {}
                d = wj.get("description", "")
                if isinstance(d, dict):
                    d = d.get("value", "")
                desc = d or desc

        if not cover:
            # /isbn sometimes has a covers[] list of b-ids
            if bj.get("covers"):
                cover_id = bj["covers"][0]
                cover = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
            else:
                # Final ISBN-based cover attempt
                cover = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"

        # Language
        lang = ""
        try:
            lang_key = (data.get("languages", [{}])[0].get("key"," ").split("/")[-1]).upper()
            lang = lang_key
        except Exception:
            pass
        if not lang:
            try:
                langs = bj.get("languages", [])
                if langs:
                    lang = (langs[0].get("key"," ").split("/")[-1] or "").upper()
            except Exception:
                lang = ""

        return {
            "ISBN": isbn,
            "Title": (data.get("title","") or "").strip(),
            "Author": author,
            "Genre": subjects,
            "Language": lang,
            "Thumbnail": cover or "",
            "Description": (desc or "").strip(),
            "PublishedDate": data.get("publish_date",""),
        }
    except Exception:
        return {}

def get_goodreads_rating_placeholder(isbn: str) -> str:
    return "GR:unavailable"

@st.cache_data(ttl=86400)
def get_book_metadata(isbn: str) -> dict:
    google_meta = get_book_details_google(isbn)
    openlibrary_meta = get_book_details_openlibrary(isbn)

    # Prefer Google if it returned a title; fill gaps with OL
    meta = google_meta.copy() if google_meta.get("Title") else openlibrary_meta.copy()

    # Backfill from the other source where missing
    for key in ["Title", "Author", "Genre", "Language", "Thumbnail", "Description", "PublishedDate"]:
        if not meta.get(key):
            meta[key] = openlibrary_meta.get(key, "") if meta is google_meta else google_meta.get(key, "")

    # Ensure required keys exist
    for k in ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
        meta.setdefault(k, "")

    # Improve language readability
    meta["Language"] = _pretty_lang(meta.get("Language", ""))

    # Thumbnail: final fallback via OL ISBN cover
    isbn = meta.get("ISBN", "")
    if not meta.get("Thumbnail") and isbn:
        meta["Thumbnail"] = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"

    # Ratings merge: Google + OpenLibrary + placeholder
    ratings_parts = []
    if google_meta.get("Rating"):
        ratings_parts.append(f"GB:{google_meta['Rating']}")
    ol_avg, _ = get_openlibrary_rating(isbn)
    if ol_avg is not None:
        try:
            ratings_parts.append(f"OL:{round(float(ol_avg), 2)}")
        except Exception:
            ratings_parts.append(f"OL:{ol_avg}")
    ratings_parts.append(get_goodreads_rating_placeholder(isbn))
    meta["Rating"] = " | ".join(ratings_parts)

    # Known name fix
    if meta.get("Author") == "Jø Lier Horst":
        meta["Author"] = "Jørn Lier Horst"

    meta["Author"] = keep_primary_author(meta.get("Author", ""))

    return meta

# ---------- Recommendations (two modes) ----------
@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list[dict]:
    if not author:
        return []
    results: list[dict] = []

    # Try Google Books first
    try:
        params = {"q": f"inauthor:{author}", "printType": "books", "maxResults": 20, "orderBy": "relevance"}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=12, headers=UA)
        if r.ok:
            for item in r.json().get("items", []) or []:
                vi = item.get("volumeInfo", {})
                isbn = ""
                for ident in vi.get("industryIdentifiers", []) or []:
                    if ident.get("type") in ("ISBN_13", "ISBN_10"):
                        isbn = ident.get("identifier", "")
                        break
                thumb = (vi.get("imageLinks") or {}).get("thumbnail", "")
                if thumb.startswith("http://"):
                    thumb = thumb.replace("http://", "https://")
                results.append({
                    "source": "google",
                    "title": vi.get("title", ""),
                    "authors": ", ".join(vi.get("authors", [])) if vi.get("authors") else "",
                    "isbn": isbn,
                    "published": vi.get("publishedDate", ""),
                    "description": vi.get("description", "") or "",
                    "thumbnail": thumb,
                })
    except Exception:
        pass

    if results:
        return results

    # Fallback: OpenLibrary search
    try:
        ro = requests.get("https://openlibrary.org/search.json", params={"author": author, "limit": 20}, timeout=12, headers=UA)
        if ro.ok:
            data = ro.json()
            for doc in data.get("docs", []) or []:
                isbn = (doc.get("isbn") or [""])[0]
                cover_id = doc.get("cover_i")
                if cover_id:
                    thumb = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg"
                elif isbn:
                    thumb = f"https://covers.openlibrary.org/b/isbn/{isbn}-M.jpg"
                else:
                    thumb = ""
                results.append({
                    "source": "openlibrary",
                    "title": doc.get("title", ""),
                    "authors": ", ".join(doc.get("author_name", []) or []),
                    "isbn": isbn,
                    "published": str(doc.get("first_publish_year", "")),
                    "description": "",
                    "thumbnail": thumb,
                })
    except Exception:
        pass

    return results

# ---------- UI helpers ----------

def _cover_or_placeholder(url: str, title: str = "") -> tuple[str, str]:
    url = (url or "").strip()
    if url:
        return url, title or ""
    txt = quote((title or "No Cover").upper().replace(" ", "\n"))
    placeholder = f"https://via.placeholder.com/300x450/FFFFFF/000000?text={txt}"
    return placeholder, (title or "No Cover")

def append_record(tab: str, record: dict) -> None:
    """Ensure headers, dedupe, and append a record to the specified sheet."""
    try:
        ws = _get_ws(tab)
        if not ws:
            raise RuntimeError("Worksheet not found")

        headers = [h.strip() for h in ws.row_values(1)]
        if not headers:
            headers = EXACT_HEADERS[:]
            ws.update('A1', [headers])
        else:
            extras = [h for h in headers if h not in EXACT_HEADERS]
            headers = EXACT_HEADERS[:] + extras
            ws.update('A1', [headers])

        values = ws.get_all_values()
        existing_isbns, existing_ta = set(), set()
        i_isbn = headers.index("ISBN") if "ISBN" in headers else None
        i_title = headers.index("Title") if "Title" in headers else None
        i_author = headers.index("Author") if "Author" in headers else None

        for r in values[1:]:
            if i_isbn is not None and len(r) > i_isbn:
                norm = _normalize_isbn(r[i_isbn])
                if norm:
                    existing_isbns.add(norm)
            if i_title is not None and i_author is not None and len(r) > max(i_title, i_author):
                t = (r[i_title] or "").strip().lower()
                a = (r[i_author] or "").strip().lower()
                if t or a:
                    existing_ta.add((t, a))

        inc_isbn_norm = _normalize_isbn(record.get("ISBN", ""))
        inc_ta = ((record.get("Title", "").strip().lower()), (record.get("Author", "").strip().lower()))

        if inc_isbn_norm and inc_isbn_norm in existing_isbns:
            st.info(f"'{record.get('Title','(unknown)')}' is already in {tab} (same ISBN). Skipped.")
            return
        if inc_ta in existing_ta:
            st.info(f"'{record.get('Title','(unknown)')}' by {record.get('Author','?')} is already in {tab}. Skipped.")
            return

        if record.get("ISBN") and str(record["ISBN"]).isdigit():
            record["ISBN"] = "'" + str(record["ISBN"]).strip()

        keymap = {h.lower(): h for h in headers}
        row = [record.get(keymap.get(h.lower(), h), record.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()

    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")
        raise

# ---------- Reusable Grid Display Function ----------
# ---------- Reusable Grid Display Function (Fixed 3-Column View) ----------
def display_books_grid(df: pd.DataFrame, show_read_status: bool = False):
    """Renders a DataFrame of books as a responsive, styled grid of covers."""
    if df.empty:
        return

    checkmark_svg = (
        "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' "
        "fill='white'%3E%3Cpath d='M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z'/%3E%3C/svg%3E"
    )

    # CSS for styling the grid, covers, and checkmark overlay
    st.markdown(f"""
        <style>
            .book-grid {{
                display: grid;
                /* THIS IS THE CHANGED LINE to force 3 columns */
                grid-template-columns: repeat(3, 1fr);
                gap: 1rem; /* A slightly larger gap looks better with 3 columns */
            }}
            .book-container {{
                position: relative;
                transition: transform 0.2s;
            }}
            .book-container:hover {{
                transform: scale(1.05);
            }}
            .book-cover {{
                width: 100%;
                border-radius: 8px;
                box-shadow: 2px 2px 8px rgba(0,0,0,0.2);
            }}
            .read-check {{
                position: absolute;
                bottom: 8px;
                right: 8px;
                width: 28px;
                height: 28px;
                background-color: rgba(45, 186, 75, 0.9);
                border-radius: 50%;
                background-image: url("{checkmark_svg}");
                background-size: 60%;
                background-repeat: no-repeat;
                background-position: center;
                border: 2px solid white;
                box-shadow: 0 0 8px rgba(0,0,0,0.5);
            }}
        </style>
    """, unsafe_allow_html=True)

    for col in ["Thumbnail", "Title", "Author", "Date Read"]:
        if col not in df.columns:
            df[col] = ""
    df = df.fillna("")

    st.write('<div class="book-grid">', unsafe_allow_html=True)
    for _, book in df.iterrows():
        cover_url, _ = _cover_or_placeholder(book["Thumbnail"], book["Title"])
        is_read = show_read_status and book["Date Read"].strip() != ""
        
        html = f'<div class="book-container">'
        html += f'<img src="{cover_url}" class="book-cover" alt="{book["Title"]}">'
        if is_read:
            html += '<div class="read-check"></div>'
        html += f'</div>'
        
        st.markdown(html, unsafe_allow_html=True)
        
    st.write('</div>', unsafe_allow_html=True)

# ---------- UI ----------

st.title("Misiddons Book Database")

# Initialize session state for form and scanner if not present
for k, v in {
    "scan_isbn": "", "scan_title": "", "scan_author": "", "last_scan_meta": {},
}.items():
    st.session_state.setdefault(k, v)

# --- Add Book Form ---
with st.expander("✍️ Add a New Book Manually", expanded=False):
    with st.form("entry_form"):
        cols = st.columns(5)
        title = cols[0].text_input("Title", value=st.session_state.get("scan_title", ""))
        author = cols[1].text_input("Author", value=st.session_state.get("scan_author", ""))
        isbn = cols[2].text_input("ISBN (Optional)", value=st.session_state.get("scan_isbn", ""))
        date_read = cols[3].text_input("Date Read", placeholder="YYYY/MM/DD")
        choice = cols[4].radio("Add to:", ["Library", "Wishlist"], horizontal=True)

        if st.form_submit_button("Add Book"):
            if title and author:
                try:
                    scan_meta = st.session_state.get("last_scan_meta", {})
                    rec = {"ISBN": isbn, "Title": title, "Author": author, "Date Read": date_read}
                    for k in ["Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
                        if k in scan_meta and scan_meta[k]:
                            rec[k] = scan_meta[k]

                    lib_df = load_data("Library")
                    wish_df = load_data("Wishlist")

                    for df in (lib_df, wish_df):
                        if not df.empty:
                            for col in ["ISBN","Title","Author"]:
                                if col not in df.columns: df[col] = ""

                    all_df = pd.concat([lib_df, wish_df], ignore_index=True) if not lib_df.empty or not wish_df.empty else pd.DataFrame(columns=["ISBN","Title","Author"])

                    existing_isbns = set(all_df["ISBN"].astype(str).map(_normalize_isbn).dropna()) if not all_df.empty else set()
                    existing_ta = set(zip(
                        all_df.get("Title", pd.Series(dtype=str)).fillna("").str.strip().str.lower(),
                        all_df.get("Author", pd.Series(dtype=str)).fillna("").str.strip().str.lower(),
                    )) if not all_df.empty else set()

                    inc_isbn_norm = _normalize_isbn(rec.get("ISBN",""))
                    inc_ta = (rec.get("Title","").strip().lower(), rec.get("Author","").strip().lower())

                    if inc_isbn_norm and inc_isbn_norm in existing_isbns:
                        st.warning(f"This book (ISBN: {rec.get('ISBN','')}) already exists. Skipped.")
                    elif inc_ta in existing_ta:
                        st.warning(f"'{rec['Title']}' by {rec['Author']} already exists. Skipped.")
                    else:
                        append_record(choice, rec)
                        st.success(f"Added '{title}' to {choice} 🎉")
                        for k in ("scan_isbn","scan_title","scan_author"): st.session_state[k] = ""
                        st.session_state["last_scan_meta"] = {}
                        st.rerun()
                except Exception as e:
                    st.error(f"Failed to add book: {e}")
            else:
                st.warning("Enter both a title and author to add a book.")

# --- Barcode scanner (from image) ---
if zbar_decode:
    with st.expander("📷 Scan Barcode from Photo", expanded=False):
        up = st.file_uploader("Upload a clear photo of the barcode", type=["png", "jpg", "jpeg"])
        if up:
            try:
                img = Image.open(up)
                if img.mode != "RGB": img = img.convert("RGB")
                codes = zbar_decode(img)
            except Exception:
                codes = []

            if not codes:
                st.warning("No barcode found. Please try a closer, sharper photo.")
            else:
                raw = codes[0].data.decode(errors="ignore")
                digits = "".join(ch for ch in raw if ch.isdigit())
                isbn_bc = digits[-13:] if len(digits) >= 13 else digits
                st.info(f"Detected code: {raw} → Using ISBN: {isbn_bc}")

                with st.spinner("Fetching book details..."):
                    meta = get_book_metadata(isbn_bc)

                if not meta or not meta.get("Title"):
                    st.error("Couldn't fetch details. Check the ISBN or try again.")
                else:
                    st.session_state.update({
                        "scan_isbn": meta.get("ISBN", ""),
                        "scan_title": meta.get("Title", ""),
                        "scan_author": meta.get("Author", ""),
                        "last_scan_meta": meta
                    })

                    cols = st.columns([1, 3])
                    with cols[0]:
                        cover_url, cap = _cover_or_placeholder(meta.get("Thumbnail",""), meta.get("Title",""))
                        st.image(cover_url, caption=cap, width=150)
                    with cols[1]:
                        st.subheader(meta.get("Title","Unknown Title"))
                        st.write(f"**Author:** {meta.get('Author','Unknown')}")
                        st.write(f"**Published Date:** {meta.get('PublishedDate','Unknown')}")
                        if meta.get("Rating"): st.write(f"**Rating:** {meta.get('Rating')}")
                        if meta.get("Language"): st.write(f"**Language:** {normalize_language(meta.get('Language'))}")

                    full_desc = meta.get("Description", "")
                    if full_desc:
                        with st.expander("Description", expanded=len(full_desc) < 500):
                            st.write(full_desc)

                    a1, a2 = st.columns(2)
                    with a1:
                        if st.button("➕ Add to Library", key="add_scan_lib", use_container_width=True):
                            try:
                                append_record("Library", meta)
                                st.success("Added to Library 🎉")
                                st.session_state.update({"scan_isbn": "", "scan_title": "", "scan_author": "", "last_scan_meta": {}})
                                st.rerun()
                            except Exception: pass
                    with a2:
                        if st.button("🧾 Add to Wishlist", key="add_scan_wl", use_container_width=True):
                            try:
                                append_record("Wishlist", meta)
                                st.success("Added to Wishlist 📝")
                                st.session_state.update({"scan_isbn": "", "scan_title": "", "scan_author": "", "last_scan_meta": {}})
                                st.rerun()
                            except Exception: pass
else:
    st.info("Barcode scanning requires `pyzbar`/`zbar`. If unavailable, paste the ISBN manually or use the manual form.")

st.divider()

# --- Tabs ---
tabs = st.tabs(["Library", "Wishlist", "Statistics", "Recommendations"])

with tabs[0]:
    st.title("Library")
    library_df = load_data("Library")

    if library_df.empty:
        st.info("Your library is empty. Add a book to get started!")
    else:
        search_query = st.text_input("Search this collection...", key="lib_search", placeholder="Search titles or authors...")

        df_display = library_df.copy()
        if search_query:
            df_display = df_display[df_display.apply(
                lambda row: row.astype(str).str.contains(search_query, case=False, na=False).any(), axis=1
            )]

        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"**{len(df_display)} BOOKS**")
        with col2:
            sort_options = {"Title": "Title", "Author": "Author", "Date Read": "Date Read"}
            sort_by = st.selectbox("SORT", options=sort_options.keys(), index=0, key="lib_sort")

            if sort_by == "Date Read":
                df_display["_sort_date"] = pd.to_datetime(df_display["Date Read"], errors='coerce')
                df_display = df_display.sort_values(by="_sort_date", ascending=False).drop(columns=["_sort_date"])
            else:
                df_display = df_display.sort_values(by=sort_options[sort_by], ascending=True)

        display_books_grid(df_display, show_read_status=True)

with tabs[1]:
    st.title("Wishlist")
    wishlist_df = load_data("Wishlist")

    if wishlist_df.empty:
        st.info("Your wishlist is empty. Scan a book or add one manually!")
    else:
        search_query = st.text_input("Search this collection...", key="wish_search", placeholder="Search titles or authors...")
        
        df_display = wishlist_df.copy()
        if search_query:
            df_display = df_display[df_display.apply(
                lambda row: row.astype(str).str.contains(search_query, case=False, na=False).any(), axis=1
            )]
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"**{len(df_display)} BOOKS**")
        with col2:
            sort_options = {"Title": "Title", "Author": "Author"}
            sort_by = st.selectbox("SORT", options=sort_options.keys(), index=0, key="wish_sort")
            df_display = df_display.sort_values(by=sort_options[sort_by], ascending=True)

        display_books_grid(df_display, show_read_status=False)

with tabs[2]:
    st.header("Statistics")
    library_df = load_data("Library")
    wishlist_df = load_data("Wishlist")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Books in Library", len(library_df))
    with col2:
        st.metric("Total Books on Wishlist", len(wishlist_df))
    with col3:
        uniq_auth = 0 if library_df.empty or "Author" not in library_df.columns else library_df["Author"].fillna("").astype(str).str.split(",").explode().str.strip().replace({"": None}).dropna().nunique()
        st.metric("Unique Authors (Library)", int(uniq_auth))

with tabs[3]:
    st.header("Recommendations")
    library_df = load_data("Library")
    wishlist_df = load_data("Wishlist")

    owned_titles, owned_isbns = set(), set()
    for df in (library_df, wishlist_df):
        if not df.empty:
            if "Title" in df.columns: owned_titles.update(df["Title"].dropna().astype(str).str.lower().str.strip().tolist())
            if "ISBN" in df.columns: owned_isbns.update(df["ISBN"].dropna().astype(str).map(_normalize_isbn).tolist())

    authors = []
    if not library_df.empty and "Author" in library_df.columns:
        authors = sorted(set(
            library_df["Author"].dropna().astype(str).str.split(",").explode().str.strip().replace({"": None}).dropna().tolist()
        ), key=str.lower)

    mode = st.radio("Recommendation mode:", ["Surprise me (4 random unseen)", "By author"], horizontal=True)

    if mode == "By author":
        selected_author = st.selectbox("Find books by authors you've read:", authors) if authors else st.text_input("Type an author to get recommendations:")
        if selected_author:
            recommendations = get_recommendations_by_author(selected_author)
            shown = 0
            for item in recommendations:
                title, isbn = (item.get("title") or "").strip(), _normalize_isbn(item.get("isbn", ""))
                if (title.lower() in owned_titles) or (isbn and isbn in owned_isbns): continue
                
                cols = st.columns([1, 4])
                with cols[0]:
                    thumb, _ = _cover_or_placeholder(item.get("thumbnail", ""), title)
                    st.image(thumb, width=100)
                with cols[1]:
                    st.subheader(title or "No Title")
                    st.write(f"**Author(s):** {item.get('authors', 'N/A')}")
                    st.write(f"**Published:** {item.get('published', 'N/A')}")
                    if item.get("description"): st.caption(item["description"]) 

                    if st.button("🧾 Add to Wishlist", key=f"rec_add_{selected_author}_{shown}"):
                        rec_meta = {k: item.get(k, "") for k in ["ISBN", "Title", "Author", "Thumbnail", "Description"]}
                        rec_meta.update({"Genre": "", "Language": "", "Rating": "", "PublishedDate": item.get("published", "")})
                        try:
                            append_record("Wishlist", rec_meta)
                            st.success(f"Added '{title}' to Wishlist")
                            st.rerun()
                        except Exception as e: st.error(f"Could not add: {e}")
                    st.markdown("---")
                shown += 1
                if shown >= 5: break
            if shown == 0: st.info("No new recommendations found. Try another author.")
    else:
        if not authors:
            st.info("Add books to your Library to get surprise recommendations.")
        else:
            sample_authors = random.sample(authors, k=min(6, len(authors)))
            pool = [item for a in sample_authors for item in get_recommendations_by_author(a)]
            
            filtered = [
                item for item in pool if (item.get("title") or "").strip() and
                (item.get("title","").strip().lower() not in owned_titles) and
                not (_normalize_isbn(item.get("isbn","")) and _normalize_isbn(item.get("isbn","")) in owned_isbns)
            ]
            random.shuffle(filtered)
            picks = filtered[:4]

            if not picks:
                st.info("Couldn't find unseen picks. Try switching to 'By author' mode.")
            for idx, item in enumerate(picks, 1):
                title = (item.get("title") or "").strip()
                cols = st.columns([1, 4])
                with cols[0]:
                    thumb, _ = _cover_or_placeholder(item.get("thumbnail", ""), title)
                    st.image(thumb, width=100)
                with cols[1]:
                    st.subheader(f"{idx}. {title or 'No Title'}")
                    st.write(f"**Author(s):** {item.get('authors', 'N/A')}")
                    st.write(f"**Published:** {item.get('published', 'N/A')}")
                    if item.get("description"): st.caption(item["description"]) 

                    if st.button("🧾 Add to Wishlist", key=f"rec_surprise_add_{idx}"):
                        rec_meta = {k: item.get(k, "") for k in ["ISBN", "Title", "Author", "Thumbnail", "Description"]}
                        rec_meta.update({"Genre": "", "Language": "", "Rating": "", "PublishedDate": item.get("published", "")})
                        try:
                            append_record("Wishlist", rec_meta)
                            st.success(f"Added '{title}' to Wishlist")
                            st.rerun()
                        except Exception as e: st.error(f"Could not add: {e}")
                st.markdown("---")

# ---- Diagnostics and Data Checkers ----
with st.expander("Diagnostics"):
    try:
        acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
        st.write("Service account email:", acct)
        st.write("Spreadsheet ID in use:", SPREADSHEET_ID)
        try:
            test_client = connect_to_gsheets()
            if test_client:
                ss = test_client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else test_client.open(GOOGLE_SHEET_NAME)
                st.write("Found worksheet tabs:", [w.title for w in ss.worksheets()])
        except Exception as e:
            st.write("Open spreadsheet error:", f"{type(e).__name__}: {e}")
    except Exception as e:
        st.write("Diagnostics error:", f"{type(e).__name__}: {e}")

with st.expander("🔍 Data Check — Library", expanded=False):
    lib = load_data("Library")
    if lib.empty:
        st.info("Library sheet is empty.")
    else:
        for c in ["ISBN","Title","Author","Language","Thumbnail","PublishedDate","Date Read","Description"]:
            if c not in lib.columns: lib[c] = ""
        lib["_isbn_norm"] = lib["ISBN"].astype(str).map(_normalize_isbn)
        lib["_author_primary"] = lib["Author"].astype(str).map(keep_primary_author)
        lib["_title_norm"] = lib["Title"].astype(str).str.strip().str.lower()
        lib["_ta_key"] = lib["_title_norm"] + " | " + lib["_author_primary"].str.strip().str.lower()
        issues = []
        mask_missing = (lib["Title"].astype(str).str.strip() == "") | (lib["Author"].astype(str).str.strip() == "")
        for i, r in lib[mask_missing].iterrows():
            issues.append({"Row": i+2, "Issue": "Missing Title or Author", "Title": r["Title"], "Author": r["Author"], "ISBN": r["ISBN"], "Suggestion": "Fill in missing field(s)."})
        if issues:
            prob_df = pd.DataFrame(issues, columns=["Row","Issue","Title","Author","ISBN","Suggestion"])
            st.warning(f"Found {len(prob_df)} potential issue(s).")
            st.dataframe(prob_df, use_container_width=True, hide_index=True)
        else:
            st.success("Looks good! No issues detected in Library 🎉")

def _strip_diacritics(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()

def _norm_title(s: str) -> str:
    s = _strip_diacritics(str(s))
    s = re.split(r"[:(\\[]", s, 1)[0]
    s = re.sub(r"\\b(a|an|the)\\b\\s+", "", s, flags=re.I)
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    s = re.sub(r"\\s+", " ", s).strip()
    return s

def _norm_author(s: str) -> str:
    s = keep_primary_author(str(s))
    s = _strip_diacritics(s).replace("&", "and")
    s = re.sub(r"[^a-z ]+", " ", s.lower())
    s = re.sub(r"\\s+", " ", s).strip()
    return s

@st.cache_data(ttl=86400)
def _search_google_by_ta(title: str, author: str) -> dict:
    try:
        q = f'intitle:"{title}" inauthor:"{author}"'
        params = {"q": q, "printType": "books", "maxResults": 1}
        if GOOGLE_BOOKS_KEY: params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=12, headers=UA)
        if r.ok and r.json().get("items"):
            vi = r.json()["items"][0].get("volumeInfo", {})
            au = (vi.get("authors") or [])
            return {"source": "google-search", "Title": (vi.get("title") or "").strip(), "Author": keep_primary_author(au[0].strip()) if au else ""}
    except Exception:
        pass
    return {}

@st.cache_data(ttl=86400)
def _search_ol_by_ta(title: str, author: str) -> dict:
    try:
        r = requests.get("https://openlibrary.org/search.json", params={"title": title, "author": author, "limit": 1}, timeout=12, headers=UA)
        if r.ok:
            docs = (r.json().get("docs") or [])
            if docs:
                au = (docs[0].get("author_name") or [])
                return {"source": "ol-search", "Title": (docs[0].get("title") or "").strip(), "Author": keep_primary_author(au[0].strip()) if au else ""}
    except Exception:
        pass
    return {}

@st.cache_data(ttl=86400)
def _canonical_from_row(title: str, author: str, isbn: str) -> dict:
    """Prefer ISBN lookups; fall back to title+author search."""
    isbn = _normalize_isbn(isbn)
    if isbn:
        g = get_book_details_google(isbn)
        if g.get("Title"):
            return {"source": "google-isbn", "Title": g["Title"], "Author": g["Author"]}
        o = get_book_details_openlibrary(isbn)
        if o.get("Title"):
            return {"source": "ol-isbn", "Title": o["Title"], "Author": o["Author"]}
    s = _search_google_by_ta(title, author) or _search_ol_by_ta(title, author)
    return s or {}

with st.expander("🔎 Cross-check — Authors & Titles (Library)", expanded=False):
    lib = load_data("Library")
    if lib.empty:
        st.info("Library sheet is empty.")
    else:
        for c in ["ISBN", "Title", "Author"]:
            if c not in lib.columns:
                lib[c] = ""
        rows = []
        issues = []
        for i, r in lib.iterrows():
            sheet_title  = str(r["Title"]).strip()
            sheet_author = str(r["Author"]).strip()
            sheet_isbn   = str(r["ISBN"]).strip()
            if not sheet_title and not sheet_author:
                continue
            can = _canonical_from_row(sheet_title, sheet_author, sheet_isbn)
            if not can:
                rows.append({"Row": i+2, "ISBN": sheet_isbn, "Sheet Title": sheet_title, "Sheet Author": sheet_author, "Canonical Title": "(not found)", "Canonical Author": "(not found)", "Title Match": "n/a", "Author Match": "n/a", "Source": "n/a", "Note": "No external match"})
                continue
            nt_s, nt_c = _norm_title(sheet_title), _norm_title(can["Title"])
            na_s, na_c = _norm_author(sheet_author), _norm_author(can["Author"])
            t_ratio = SequenceMatcher(None, nt_s, nt_c).ratio() if nt_c else 0.0
            a_ratio = SequenceMatcher(None, na_s, na_c).ratio() if na_c else 0.0
            t_match = "exact" if nt_s == nt_c else ("close" if t_ratio >= 0.85 else "diff")
            a_match = "exact" if na_s == na_c else ("close" if a_ratio >= 0.85 else "diff")
            note = ""
            if t_match == "diff": note += "Title differs. "
            if a_match == "diff": note += "Author differs. "
            if not note and (t_match == "close" or a_match == "close"): note = "Minor variance (edition/subtitle/diacritics)."
            row_info = {"Row": i+2, "ISBN": sheet_isbn, "Sheet Title": sheet_title, "Canonical Title": can["Title"], "Title Match": t_match, "Sheet Author": sheet_author, "Canonical Author": can["Author"], "Author Match": a_match, "Source": can.get("source",""), "Note": note.strip()}
            rows.append(row_info)
            if t_match != "exact" or a_match != "exact":
                issues.append(row_info)
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        if issues:
            st.warning(f"{len(issues)} row(s) need attention. Look at 'diff' rows and update the sheet if needed.")
        else:
            st.success("All titles & authors match the external sources 🎯")
