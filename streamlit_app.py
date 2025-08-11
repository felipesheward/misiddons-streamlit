#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Form + Scanner)
- Add books manually via form
- Scan barcodes from a photo to auto-fill metadata (title, author(s), cover, description)
- Add to Library or Wishlist
- Prevents duplicates (by ISBN or Title+Author)
- Recommends up to 4 books you DON'T already have (based on Library + Wishlist authors & subjects)
"""
from __future__ import annotations

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
except Exception:
    zbar_decode = None

# ---------- CONFIG ----------
DEFAULT_SHEET_ID = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"
SPREADSHEET_ID = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")
GOOGLE_BOOKS_KEY = st.secrets.get("google_books_api_key", None)

st.set_page_config(page_title="Misiddons Book Database", layout="wide")

# For Streamlit >1.30 the proper call is st.rerun
_rerun = getattr(st, "rerun", getattr(st, "experimental_rerun", None))

# ---------- Small utils ----------
def _first_nonempty(*vals: str) -> str:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _split_authors_for_seeds(author_field: str) -> list[str]:
    """
    Split an Author cell into individual names for seeds.
    Handles 'A, B', 'A & B', 'A and B'.
    """
    if not author_field:
        return []
    s = str(author_field)
    for token in [" & ", " and "]:
        s = s.replace(token, ", ")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return [p for p in parts if len(p) >= 2]

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
    ss = None
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
        try:
            df = pd.DataFrame(ws.get_all_records())
            return df.dropna(how="all")
        except Exception:
            vals = ws.get_all_values()
            if not vals:
                return pd.DataFrame()
            header, *rows = vals
            return pd.DataFrame(rows, columns=header).dropna(how="all")
    except WorksheetNotFound:
        try:
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

@st.cache_data(ttl=60)
def _get_ws(tab: str):
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
    "ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate","Date Read"
]

ISO_LANG = {
    "EN":"English","IT":"Italian","ES":"Spanish","DE":"German","FR":"French",
    "PT":"Portuguese","NL":"Dutch","SV":"Swedish","NO":"Norwegian","DA":"Danish",
    "FI":"Finnish","RU":"Russian","PL":"Polish","TR":"Turkish","ZH":"Chinese",
    "JA":"Japanese","KO":"Korean","AR":"Arabic","HE":"Hebrew","HI":"Hindi"
}

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

def append_record(tab: str, record: dict) -> None:
    """Ensure headers, dedupe (ISBN or Title+Author), preserve ISBN as text, then append."""
    try:
        ws = _get_ws(tab)
        if not ws:
            raise RuntimeError("Worksheet not found")
        # 1) Ensure headers in a fixed order (keep any extras at the end)
        headers = [h.strip() for h in ws.row_values(1)]
        if not headers:
            headers = EXACT_HEADERS[:]
            ws.update('A1', [headers])
        else:
            extras = [h for h in headers if h not in EXACT_HEADERS]
            headers = EXACT_HEADERS[:] + extras
            ws.update('A1', [headers])

        # 2) De-dup in this tab
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

        # 3) Preserve ISBN as text, build row in header order, append
        if record.get("ISBN") and str(record["ISBN"]).isdigit():
            record["ISBN"] = "'" + str(record["ISBN"]).strip()
        keymap = {h.lower(): h for h in headers}
        row = [record.get(keymap.get(h.lower(), h), record.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="RAW")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")
        raise

# ---------- Metadata fetchers ----------
@st.cache_data(ttl=86400)
def get_book_details_google(isbn: str) -> dict:
    """Google Books metadata. Keep FULL author list joined for reliability."""
    if not isbn:
        return {}
    try:
        params = {"q": f"isbn:{isbn}", "printType": "books", "maxResults": 1}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY

        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params=params,
            timeout=15,
            headers={"User-Agent": "misiddons/1.0", "Accept": "application/json"},
        )
        r.raise_for_status()
        items = r.json().get("items", []) or []
        if not items:
            return {}

        info = items[0].get("volumeInfo", {}) or {}
        desc = info.get("description") or items[0].get("searchInfo", {}).get("textSnippet") or ""

        # Authors: keep the full list joined (split later for seeds)
        authors_list = [a for a in (info.get("authors") or []) if a and a.strip()]
        author_str = ", ".join(authors_list) if authors_list else ""

        links = info.get("imageLinks", {}) or {}
        thumb = links.get("thumbnail") or links.get("smallThumbnail") or ""
        if thumb.startswith("http://"):
            thumb = thumb.replace("http://", "https://")

        cats = info.get("categories") or []
        return {
            "ISBN": isbn,
            "Title": info.get("title", "") or "",
            "Author": author_str,  # full list joined
            "Genre": ", ".join(cats) if cats else "",
            "Language": (info.get("language") or "").upper(),
            "Thumbnail": thumb,
            "Description": desc.strip(),
            "Rating": str(info.get("averageRating", "")) if info.get("averageRating") is not None else "",
            "PublishedDate": info.get("publishedDate", "") or "",
        }
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def _ol_fetch_json(url: str) -> dict:
    try:
        r = requests.get(url, timeout=12, headers={"User-Agent": "misiddons/1.0"})
        if r.ok:
            return r.json()
    except Exception:
        pass
    return {}

@st.cache_data(ttl=86400)
def get_openlibrary_rating(isbn: str):
    """Return (avg, count) rating for the book's first work on Open Library, if any."""
    try:
        bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json") or {}
        works = bj.get("works") or []
        if not works:
            return None, None
        work_key = works[0].get("key")
        if not work_key:
            return None, None
        rj = _ol_fetch_json(f"https://openlibrary.org{work_key}/ratings.json") or {}
        summary = rj.get("summary", {}) if isinstance(rj, dict) else {}
        return (summary.get("average"), summary.get("count"))
    except Exception:
        return None, None

@st.cache_data(ttl=86400)
def get_book_details_openlibrary(isbn: str) -> dict:
    """OpenLibrary metadata with solid author resolution and robust fallbacks."""
    try:
        # Rich 'data' endpoint
        r = requests.get(
            "https://openlibrary.org/api/books",
            params={"bibkeys": f"ISBN:{isbn}", "jscmd": "data", "format": "json"},
            timeout=15,
            headers={"User-Agent": "misiddons/1.0", "Accept": "application/json"},
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}", {}) or {}

        # Authors from 'data'
        data_authors = [a.get("name", "") for a in (data.get("authors") or []) if isinstance(a, dict)]
        author_from_data = ", ".join([a for a in data_authors if a.strip()])

        subjects = ", ".join(s.get("name", "") for s in data.get("subjects", []) if s) or ""
        cover = (data.get("cover") or {}).get("large") or (data.get("cover") or {}).get("medium") or ""
        desc = data.get("description", "")
        if isinstance(desc, dict):
            desc = desc.get("value", "")

        # Secondary: /isbn JSON (works, authors keys, publish info)
        bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json") or {}

        # Resolve authors via /authors/* if 'data' lacked names
        author_names = []
        if author_from_data:
            author_names = [a.strip() for a in author_from_data.split(",") if a.strip()]
        else:
            for a in (bj.get("authors") or []):
                key = a.get("key")
                if not key:
                    continue
                aj = _ol_fetch_json(f"https://openlibrary.org{key}.json") or {}
                nm = aj.get("name") or aj.get("personal_name") or ""
                if nm and nm.strip():
                    author_names.append(nm.strip())
            if not author_names:
                by_stmt = data.get("by_statement") or bj.get("by_statement") or ""
                if by_stmt:
                    by_stmt = by_stmt.strip()
                    if by_stmt.lower().startswith("by "):
                        by_stmt = by_stmt[3:].strip()
                    if by_stmt:
                        author_names = [by_stmt]

        # Works description fallback
        if not desc:
            works = bj.get("works") or []
            if works and works[0].get("key"):
                wk = works[0]["key"]
                wj = _ol_fetch_json(f"https://openlibrary.org{wk}.json") or {}
                d = wj.get("description", "")
                if isinstance(d, dict):
                    d = d.get("value", "")
                desc = d or desc

        if not cover:
            if bj.get("covers"):
                try:
                    cover_id = bj["covers"][0]
                    cover = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
                except Exception:
                    pass
            if not cover:
                cover = f"https://covers.openlibrary.org/b/ISBN/{isbn}-L.jpg"

        # Language
        lang = ""
        try:
            lang = (data.get("languages", [{}])[0].get("key", "").split("/")[-1] or "").upper()
        except Exception:
            pass
        if not lang:
            try:
                langs = bj.get("languages", [])
                if langs:
                    lang = langs[0].get("key", "").split("/")[-1].upper()
            except Exception:
                lang = ""

        published_date = data.get("publish_date", "") or bj.get("publish_date", "") or ""
        title = data.get("title", "") or bj.get("title", "") or ""

        return {
            "ISBN": isbn,
            "Title": title,
            "Author": ", ".join(author_names),  # full list joined
            "Genre": subjects,
            "Language": lang,
            "Thumbnail": cover,
            "Description": (desc or "").strip(),
            "PublishedDate": published_date,
        }
    except Exception:
        return {}

def get_goodreads_rating_placeholder(isbn: str) -> str:
    """Placeholder explaining Goodreads ratings can't be fetched (no public API)."""
    return "GR:unavailable"

def get_book_metadata(isbn: str) -> dict:
    """Merge details from Google + OpenLibrary, prefer non-empty fields."""
    google_meta = get_book_details_google(isbn)
    ol_meta     = get_book_details_openlibrary(isbn)

    meta = google_meta.copy() if google_meta else ol_meta.copy()

    # Fill empty fields from OL
    for key in ["Title","Author","Genre","Language","Thumbnail","Description","PublishedDate"]:
        if not meta.get(key) and ol_meta.get(key):
            meta[key] = ol_meta[key]

    # Ensure keys exist
    required = ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]
    for k in required:
        meta.setdefault(k, "")

    # Ratings
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
    meta["Rating"] = " | ".join([p for p in ratings_parts if p])

    # Normalize ISBN
    meta["ISBN"] = "".join(ch for ch in str(isbn) if ch.isdigit())
    return meta

@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list:
    if not author:
        return []
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"inauthor:{quote(author)}", "maxResults": 8},
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        if r.ok:
            return r.json().get("items", [])
    except Exception:
        pass
    return []

# ---------- Recommendation helpers ----------
def _extract_subjects(df: pd.DataFrame) -> list[str]:
    """Collect subjects/genres from the 'Genre' column (comma-separated)."""
    if df is None or df.empty or "Genre" not in df.columns:
        return []
    subjects = []
    for cell in df["Genre"].dropna().astype(str):
        parts = [p.strip() for p in cell.split(",") if p.strip()]
        subjects.extend(parts)
    return subjects

def _top_n(items: list[str], n: int = 5) -> list[str]:
    """Return top-N most frequent non-empty items."""
    from collections import Counter
    items = [i for i in items if str(i).strip()]
    return [k for k, _ in Counter(items).most_common(n)]

@st.cache_data(ttl=3600)
def _search_gbooks(query: str, max_results: int = 10) -> list[dict]:
    """Generic Google Books search helper."""
    try:
        params = {"q": query, "maxResults": max_results, "printType": "books"}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params=params,
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        if r.ok:
            return r.json().get("items", []) or []
    except Exception:
        pass
    return []

def _existing_keys(df: pd.DataFrame) -> tuple[set[str], set[tuple[str, str]]]:
    """Build quick lookups to avoid recommending what you already have."""
    if df is None or df.empty:
        return set(), set()
    isbns = set("".join(ch for ch in str(x).replace("'", "") if ch.isdigit())
                for x in df.get("ISBN", []) if pd.notna(x))
    ta = set((str(t).strip().lower(), str(a).strip().lower())
             for t, a in zip(df.get("Title", []), df.get("Author", [])))
    return isbns, ta

def _vi_to_meta(isbn_hint: str, vi: dict) -> dict:
    """Convert a Google Books volumeInfo dict to our meta schema."""
    isbn13 = ""
    for ident in vi.get("industryIdentifiers", []) or []:
        if ident.get("type") == "ISBN_13" and ident.get("identifier"):
            isbn13 = "".join(ch for ch in ident["identifier"] if ch.isdigit())
            break
    if not isbn13:
        isbn13 = "".join(ch for ch in str(isbn_hint) if ch.isdigit())

    authors = vi.get("authors") or []
    author = ", ".join([a for a in authors if a and a.strip()])  # keep list joined
    links = vi.get("imageLinks", {}) or {}
    thumb = links.get("thumbnail") or links.get("smallThumbnail") or ""
    if thumb.startswith("http://"):
        thumb = thumb.replace("http://", "https://")
    cats = vi.get("categories") or []

    return {
        "ISBN": isbn13,
        "Title": vi.get("title", "") or "",
        "Author": author,
        "Genre": ", ".join(cats) if cats else "",
        "Language": (vi.get("language") or "").upper(),
        "Thumbnail": thumb,
        "Description": (vi.get("description") or "").strip(),
        "Rating": str(vi.get("averageRating", "")) if vi.get("averageRating") is not None else "",
        "PublishedDate": vi.get("publishedDate", "") or "",
    }

# ---------- Barcode helpers ----------
def _extract_isbn_from_raw(raw: str) -> str:
    digits = "".join(ch for ch in raw if ch.isdigit())
    # Prefer 13-digit ISBNs that start with 978/979
    if len(digits) >= 13:
        if "978" in digits or "979" in digits:
            idx = digits.rfind("978")
            if idx == -1:
                idx = digits.rfind("979")
            if idx != -1 and idx + 13 <= len(digits):
                return digits[idx:idx+13]
        return digits[-13:]
    return digits

# ---------- UI ----------
st.title("Misiddons Book Database")

# — Add Book Form —
with st.expander("✍️ Add a New Book", expanded=False):
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
                        if scan_meta.get(k):
                            rec[k] = scan_meta[k]
                    append_record(choice, rec)
                    st.success(f"Added '{title}' to {choice}.")
                    st.session_state["scan_isbn"] = ""
                    st.session_state["scan_title"] = ""
                    st.session_state["scan_author"] = ""
                    st.session_state["last_scan_meta"] = {}
                    if _rerun:
                        _rerun()
                except Exception as e:
                    st.error(f"Failed to add book: {e}")
            else:
                st.warning("Enter both title and author.")

# — Barcode scanner (from image) —
if zbar_decode:
    with st.expander("📷 Scan Barcode from Photo"):
        up = st.file_uploader("Upload a photo of the barcode", type=["png","jpg","jpeg"])
        if up:
            try:
                img = Image.open(up)
                if img.mode != "RGB":
                    img = img.convert("RGB")
                codes = zbar_decode(img)
            except Exception:
                codes = []
            if not codes:
                st.warning("No barcode found. Try a closer, sharper photo.")
            else:
                raw = codes[0].data.decode(errors="ignore")
                isbn_bc = _extract_isbn_from_raw(raw)
                st.info(f"Detected code: {raw} → Using ISBN: {isbn_bc}")

                with st.spinner("Fetching book details..."):
                    meta = get_book_metadata(isbn_bc)

                if not meta or not meta.get("Title"):
                    st.error("Couldn't fetch details from Google/OpenLibrary. Check the ISBN or try again.")
                else:
                    st.session_state["scan_isbn"] = meta.get("ISBN", "")
                    st.session_state["scan_title"] = meta.get("Title", "")
                    st.session_state["scan_author"] = meta.get("Author", "")
                    st.session_state["last_scan_meta"] = meta

                    with st.expander("Show raw metadata", expanded=False):
                        st.json(meta)

                    cols = st.columns([1,3])
                    with cols[0]:
                        if meta.get("Thumbnail"):
                            st.image(meta["Thumbnail"], caption=meta.get("Title",""), width=150)
                    with cols[1]:
                        st.subheader(meta.get("Title","Unknown Title"))
                        st.write(f"**Author(s):** {meta.get('Author','Unknown')}")
                        st.write(f"**Published Date:** {meta.get('PublishedDate','Unknown')}")
                        if meta.get("Rating"):
                            st.write(f"**Rating:** {meta['Rating']}")
                        if meta.get("Language"):
                            st.write(f"**Language:** {meta['Language']}")

                    full_desc = meta.get("Description", "")
                    if full_desc:
                        lines = full_desc.split('\n')
                        if len(lines) > 5 or len(full_desc) > 500:
                            with st.expander("Description (click to expand)"):
                                st.write(full_desc)
                        else:
                            st.caption(full_desc)

                    a1, a2 = st.columns(2)
                    with a1:
                        if st.button("➕ Add to Library", key="add_scan_lib", use_container_width=True):
                            try:
                                append_record("Library", meta)
                                st.success("Added to Library ✔")
                                st.session_state["scan_isbn"] = ""
                                st.session_state["scan_title"] = ""
                                st.session_state["scan_author"] = ""
                                st.session_state["last_scan_meta"] = {}
                                if _rerun:
                                    _rerun()
                            except Exception:
                                pass
                    with a2:
                        if st.button("🧾 Add to Wishlist", key="add_scan_wl", use_container_width=True):
                            try:
                                append_record("Wishlist", meta)
                                st.success("Added to Wishlist ✔")
                                st.session_state["scan_isbn"] = ""
                                st.session_state["scan_title"] = ""
                                st.session_state["scan_author"] = ""
                                st.session_state["last_scan_meta"] = {}
                                if _rerun:
                                    _rerun()
                            except Exception:
                                pass
else:
    st.info("Barcode scanning requires `pyzbar`/`zbar`. If unavailable in the environment, paste the ISBN manually above.")

st.divider()

# ---- Tabs ----
tabs = st.tabs(["Library", "Wishlist", "Recommendations"])

with tabs[0]:
    st.header("My Library")
    library_df = load_data("Library")
    if not library_df.empty:
        st.dataframe(library_df, use_container_width=True)
    else:
        st.info("Library is empty.")

with tabs[1]:
    st.header("My Wishlist")
    wishlist_df = load_data("Wishlist")
    if not wishlist_df.empty:
        st.dataframe(wishlist_df, use_container_width=True)
    else:
        st.info("Wishlist is empty.")

with tabs[2]:
    st.header("Recommendations")

    library_df  = load_data("Library")
    wishlist_df = load_data("Wishlist")

    # Build seeds from BOTH tabs (robust author parsing)
    authors = []
    for df in (library_df, wishlist_df):
        if df is not None and not df.empty:
            colmap = {c.casefold(): c for c in df.columns}
            author_col = colmap.get("author")
            if author_col:
                for cell in df[author_col].dropna().astype(str).tolist():
                    authors.extend(_split_authors_for_seeds(cell))
    top_authors = _top_n(authors, n=5)

    subjects = _extract_subjects(library_df) + _extract_subjects(wishlist_df)
    top_subjects = _top_n(subjects, n=5)

    if not top_authors and not top_subjects:
        st.info("Add a few books (Library or Wishlist) to get tailored recommendations.")
    else:
        # Exclude anything already owned/wishlisted
        have_isbns_lib, have_ta_lib = _existing_keys(library_df)
        have_isbns_wl,  have_ta_wl  = _existing_keys(wishlist_df)
        have_isbns = have_isbns_lib | have_isbns_wl
        have_ta    = have_ta_lib | have_ta_wl

        # Interleave author and subject queries
        queries = []
        for a in top_authors:
            queries.append(f"inauthor:{quote(a)}")
        for s in top_subjects:
            queries.append(f"subject:{quote(s)}")

        candidates = []
        seen_ta = set()

        for q in queries:
            items = _search_gbooks(q, max_results=10)
            for item in items:
                vi = (item or {}).get("volumeInfo", {}) or {}
                meta = _vi_to_meta("", vi)
                if not meta["Title"] or not meta["Author"]:
                    continue

                ta_key = (meta["Title"].strip().lower(), meta["Author"].strip().lower())
                isbn_key = "".join(ch for ch in str(meta["ISBN"]).replace("'", "") if ch.isdigit())

                # Exclude anything already owned/wishlisted or already picked
                if (isbn_key and isbn_key in have_isbns) or (ta_key in have_ta) or (ta_key in seen_ta):
                    continue

                candidates.append(meta)
                seen_ta.add(ta_key)
                if len(candidates) >= 4:
                    break
            if len(candidates) >= 4:
                break

        if not candidates:
            st.info("No fresh recommendations right now (everything I found matches your current Library/Wishlist).")
        else:
            st.caption("Based on your most frequent authors and subjects.")
            for i, meta in enumerate(candidates, 1):
                st.markdown(f"**#{i}**")
                cols = st.columns([1, 4])
                with cols[0]:
                    if meta.get("Thumbnail"):
                        st.image(meta["Thumbnail"], width=110)
                with cols[1]:
                    st.subheader(meta.get("Title", "No Title"))
                    st.write(f"**Author(s):** {meta.get('Author','Unknown')}")
                    if meta.get("PublishedDate"):
                        st.write(f"**Published:** {meta['PublishedDate']}")
                    if meta.get("Rating"):
                        st.write(f"**Rating:** {meta['Rating']}")
                    desc = meta.get("Description", "")
                    if desc:
                        st.caption(desc if len(desc) < 280 else (desc[:280].rstrip() + "…"))

                    b1, b2 = st.columns(2)
                    with b1:
                        if st.button("➕ Add to Library", key=f"rec_add_lib_{i}", use_container_width=True):
                            try:
                                append_record("Library", meta)
                                st.success("Added to Library ✔")
                                if _rerun: _rerun()
                            except Exception as e:
                                st.error(f"Failed to add: {e}")
                    with b2:
                        if st.button("🧾 Add to Wishlist", key=f"rec_add_wl_{i}", use_container_width=True):
                            try:
                                append_record("Wishlist", meta)
                                st.success("Added to Wishlist ✔")
                                if _rerun: _rerun()
                            except Exception as e:
                                st.error(f"Failed to add: {e}")
                st.markdown("---")

# ---- Diagnostics (safe to show) ----
with st.expander("Diagnostics – help me if it still fails"):
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

        # Author completeness diagnostic
        lib = load_data("Library")
        wl  = load_data("Wishlist")
        def _missing_auth(df):
            if df is None or df.empty:
                return 0, 0
            colmap = {c.casefold(): c for c in df.columns}
            ac = colmap.get("author")
            if not ac:
                return len(df), len(df)
            miss = (df[ac].astype(str).str.strip() == "") | df[ac].isna()
            return int(miss.sum()), int(len(df))
        m_lib, n_lib = _missing_auth(lib)
        m_wl,  n_wl  = _missing_auth(wl)
        st.write(f"Missing authors — Library: {m_lib}/{n_lib}, Wishlist: {m_wl}/{n_wl}")
    except Exception as e:
        st.write("Diagnostics error:", f"{type(e).__name__}: {e}")
