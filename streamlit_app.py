#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Form + Scanner)
- Add books manually via form
- Scan barcodes from a photo to auto‑fill metadata (title, author, cover, description)
- Add to Library or Wishlist
- Prevents duplicates (by ISBN or Title+Author)
- ENHANCEMENTS (this build):
    - Search bar for filtering books
    - Improved feedback messages
    - Recommendations: two modes
        • By author (Google first, OpenLibrary fallback, filters out owned)
        • Surprise me (4 random unseen picks across your authors)
    - More readable DataFrame display
    - Authors' names with special characters handled
    - Statistics (metrics only, no chart)
    - Extra robustness in Google/OpenLibrary fetchers
    - Photo upload barcode scanner
    - NEW: Interactive Data Deep-Clean & Repair tool (v2)
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
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")
GOOGLE_BOOKS_KEY = st.secrets.get("google_books_api_key", None)

st.set_page_config(page_title="Misiddons Book Database", layout="wide")

UA = {"User-Agent": "misiddons/1.3"} # Version bump for new feature

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
            df = pd.DataFrame(ws.get_all_records(empty_value=""))
            return df.dropna(how="all").fillna("")
        except Exception:
            vals = ws.get_all_values()
            if not vals:
                return pd.DataFrame()
            header, *rows = vals
            return pd.DataFrame(rows, columns=header).dropna(how="all").fillna("")
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
    if s.count(',') >= 2:
        return s.split(',')[0].strip()
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
        return summary.get("average"), summary.get("count")
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
            "ISBN": isbn, "Title": (info.get("title", "") or "").strip(), "Author": author,
            "Genre": ", ".join(cats) if cats else "", "Language": (info.get("language") or "").upper(),
            "Thumbnail": thumb, "Description": (desc or "").strip(),
            "Rating": str(info.get("averageRating", "")), "PublishedDate": info.get("publishedDate", ""),
        }
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def get_book_details_openlibrary(isbn: str) -> dict:
    try:
        r = requests.get(
            "https://openlibrary.org/api/books",
            params={"bibkeys": f"ISBN:{isbn}", "jscmd": "data", "format": "json"},
            timeout=12, headers=UA,
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}") or {}
        authors_list = data.get("authors", [])
        author = keep_primary_author(authors_list[0].get("name", "").strip()) if authors_list else ""
        subjects = ", ".join([s.get("name","") for s in data.get("subjects", []) if s])
        cover = (data.get("cover") or {}).get("large") or (data.get("cover") or {}).get("medium") or ""
        desc = data.get("description", "")
        if isinstance(desc, dict):
            desc = desc.get("value", "")

        bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json") or {}
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
                cover = f"https://covers.openlibrary.org/b/id/{bj['covers'][0]}-L.jpg"
            else:
                cover = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"
        lang = ""
        try:
            lang_key = (data.get("languages", [{}])[0].get("key"," ").split("/")[-1]).upper()
            lang = lang_key
        except Exception: pass
        if not lang:
            try:
                langs = bj.get("languages", [])
                if langs:
                    lang = (langs[0].get("key"," ").split("/")[-1] or "").upper()
            except Exception:
                lang = ""

        return {
            "ISBN": isbn, "Title": (data.get("title","") or "").strip(), "Author": author, "Genre": subjects,
            "Language": lang, "Thumbnail": cover or "", "Description": (desc or "").strip(),
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

    meta = google_meta.copy() if google_meta.get("Title") else openlibrary_meta.copy()
    for key in ["Title", "Author", "Genre", "Language", "Thumbnail", "Description", "PublishedDate"]:
        if not meta.get(key):
            meta[key] = openlibrary_meta.get(key, "") if meta is google_meta else google_meta.get(key, "")
    for k in ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
        meta.setdefault(k, "")
    meta["Language"] = _pretty_lang(meta.get("Language", ""))
    isbn = meta.get("ISBN", "")
    if not meta.get("Thumbnail") and isbn:
        meta["Thumbnail"] = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"
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
                    "source": "google", "title": vi.get("title", ""), "authors": ", ".join(vi.get("authors", []) or []),
                    "isbn": isbn, "published": vi.get("publishedDate", ""),
                    "description": vi.get("description", "") or "", "thumbnail": thumb,
                })
    except Exception:
        pass
    if results:
        return results
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
                    "source": "openlibrary", "title": doc.get("title", ""), "authors": ", ".join(doc.get("author_name", []) or []),
                    "isbn": isbn, "published": str(doc.get("first_publish_year", "")),
                    "description": "", "thumbnail": thumb,
                })
    except Exception:
        pass
    return results

# ---------- UI helpers ----------
def _cover_or_placeholder(url: str, title: str = "") -> tuple[str, str]:
    url = (url or "").strip()
    if url:
        return url, title or ""
    txt = quote((title or "No Cover").upper())
    placeholder = f"https://via.placeholder.com/300x450?text={txt}"
    return placeholder, (title or "No Cover")

# ---------- Sheet writer ----------
def append_record(tab: str, record: dict) -> None:
    """Ensure headers, dedupe (ISBN or Title+Author), preserve ISBN as text, then append."""
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

def update_gsheet_row(tab: str, row_index: int, record: dict) -> None:
    """Finds a row by index and updates it with new data."""
    try:
        ws = _get_ws(tab)
        if not ws:
            raise RuntimeError(f"Worksheet '{tab}' not found.")
        headers = ws.row_values(1)
        if not headers:
            st.error(f"Cannot update row: worksheet '{tab}' has no headers.")
            return

        if "ISBN" in record and record.get("ISBN") and str(record["ISBN"]).isdigit():
            record["ISBN"] = "'" + str(record["ISBN"]).strip()

        row_values = [record.get(h, "") for h in headers]

        ws.update(f'A{row_index}', [row_values], value_input_option="USER_ENTERED")
        st.cache_data.clear()
        st.success(f"Row {row_index} in '{tab}' was updated successfully.")
    except Exception as e:
        st.error(f"Failed to update row {row_index} in '{tab}': {e}")
        raise

# ---------- UI ----------
st.title("Misiddons Book Database")

for k, v in {"scan_isbn": "","scan_title": "","scan_author": "","last_scan_meta": {}}.items():
    st.session_state.setdefault(k, v)

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
                                if col not in df.columns:
                                    df[col] = ""
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
                        for k in ("scan_isbn","scan_title","scan_author"):
                            st.session_state[k] = ""
                        st.session_state["last_scan_meta"] = {}
                        st.rerun()
                except Exception as e:
                    st.error(f"Failed to add book: {e}")
            else:
                st.warning("Enter both a title and author to add a book.")

if zbar_decode:
    with st.expander("📷 Scan Barcode from Photo", expanded=False):
        up = st.file_uploader("Upload a clear photo of the barcode", type=["png", "jpg", "jpeg"])
        if up:
            try:
                img = Image.open(up)
                if img.mode != "RGB":
                    img = img.convert("RGB")
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
                    st.error("Couldn't fetch details from Google/OpenLibrary. Check the ISBN or try again.")
                else:
                    st.session_state["scan_isbn"] = meta.get("ISBN", "")
                    st.session_state["scan_title"] = meta.get("Title", "")
                    st.session_state["scan_author"] = meta.get("Author", "")
                    st.session_state["last_scan_meta"] = meta
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
                        if len(full_desc.split('\n')) > 5 or len(full_desc) > 500:
                            with st.expander("Description (click to expand)"):
                                st.write(full_desc)
                        else:
                            st.caption(full_desc)
                    a1, a2 = st.columns(2)
                    with a1:
                        if st.button("➕ Add to Library", key="add_scan_lib", use_container_width=True):
                            try:
                                append_record("Library", meta)
                                for k in ("scan_isbn","scan_title","scan_author"): st.session_state[k] = ""
                                st.session_state["last_scan_meta"] = {}
                                st.rerun()
                            except Exception: pass
                    with a2:
                        if st.button("🧾 Add to Wishlist", key="add_scan_wl", use_container_width=True):
                            try:
                                append_record("Wishlist", meta)
                                for k in ("scan_isbn","scan_title","scan_author"): st.session_state[k] = ""
                                st.session_state["last_scan_meta"] = {}
                                st.rerun()
                            except Exception: pass
else:
    st.info("Barcode scanning requires `pyzbar`/`zbar`. If unavailable, paste the ISBN manually or use the manual form.")
st.divider()

tabs = st.tabs(["Library", "Wishlist", "Statistics", "Recommendations"])

with tabs[0]:
    st.header("My Library")
    library_df = load_data("Library")
    if not library_df.empty:
        search_lib = st.text_input("🔎 Search My Library...", placeholder="Search titles, authors, or genres...", key="lib_search")
        lib_df_display = library_df.copy()
        if search_lib:
            lib_df_display = lib_df_display[lib_df_display.apply(lambda r: r.astype(str).str.contains(search_lib, case=False, na=False).any(), axis=1)]
        st.dataframe(lib_df_display, use_container_width=True,
            column_config={"Thumbnail": st.column_config.ImageColumn("Cover", width="small"),
                           "Description": st.column_config.TextColumn("Description", help="Summary of the book", width="large")},
            hide_index=True)
    else:
        st.info("Your library is empty. Add a book to get started!")
with tabs[1]:
    st.header("My Wishlist")
    wishlist_df = load_data("Wishlist")
    if not wishlist_df.empty:
        search_wish = st.text_input("🔎 Search My Wishlist...", placeholder="Search titles, authors, or genres...", key="wish_search")
        wish_df_display = wishlist_df.copy()
        if search_wish:
            wish_df_display = wish_df_display[wish_df_display.apply(lambda r: r.astype(str).str.contains(search_wish, case=False, na=False).any(), axis=1)]
        st.dataframe(wish_df_display, use_container_width=True,
            column_config={"Thumbnail": st.column_config.ImageColumn("Cover", width="small"),
                           "Description": st.column_config.TextColumn("Description", help="Summary of the book", width="large")},
            hide_index=True)
    else:
        st.info("Your wishlist is empty. Scan a book or add one manually!")
with tabs[2]:
    st.header("Statistics")
    library_df = load_data("Library")
    wishlist_df = load_data("Wishlist")
    col1, col2, col3 = st.columns(3)
    with col1: st.metric("Total Books in Library", len(library_df))
    with col2: st.metric("Total Books on Wishlist", len(wishlist_df))
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
        authors = sorted(set(library_df["Author"].dropna().astype(str).str.split(",").explode().str.strip().replace({"": None}).dropna().unique().tolist()), key=str.lower)
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
                    st.image(_cover_or_placeholder(item.get("thumbnail", ""), title)[0], width=100)
                with cols[1]:
                    st.subheader(title or "No Title")
                    st.write(f"**Author(s):** {item.get('authors', 'N/A')}")
                    st.write(f"**Published:** {item.get('published', 'N/A')}")
                    if item.get("description"): st.caption(item["description"])
                    if st.button("🧾 Add to Wishlist", key=f"rec_add_{selected_author}_{shown}"):
                        rec_meta = {"ISBN": isbn, "Title": title, "Author": item.get("authors", ""),"Thumbnail": item.get("thumbnail", ""),"Description": (item.get("description") or ""),"PublishedDate": item.get("published", "")}
                        try:
                            append_record("Wishlist", rec_meta)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not add: {e}")
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
            filtered = []
            for item in pool:
                title, isbn = (item.get("title") or "").strip(), _normalize_isbn(item.get("isbn", ""))
                if not title or (title.lower() in owned_titles) or (isbn and isbn in owned_isbns): continue
                filtered.append(item)
            random.shuffle(filtered)
            picks = filtered[:4]
            if not picks:
                st.info("Couldn't find unseen picks right now. Try 'By author' mode.")
            for idx, item in enumerate(picks, 1):
                title = (item.get("title") or "").strip()
                cols = st.columns([1, 4])
                with cols[0]:
                    st.image(_cover_or_placeholder(item.get("thumbnail", ""), title)[0], width=100)
                with cols[1]:
                    st.subheader(f"{idx}. {title or 'No Title'}")
                    st.write(f"**Author(s):** {item.get('authors', 'N/A')}")
                    st.write(f"**Published:** {item.get('published', 'N/A')}")
                    if item.get("description"): st.caption(item["description"])
                    isbn = _normalize_isbn(item.get("isbn", ""))
                    if st.button("🧾 Add to Wishlist", key=f"rec_surprise_add_{idx}"):
                        rec_meta = {"ISBN": isbn, "Title": title, "Author": item.get("authors", ""),"Thumbnail": item.get("thumbnail", ""),"Description": (item.get("description") or ""),"PublishedDate": item.get("published", "")}
                        try:
                            append_record("Wishlist", rec_meta)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not add: {e}")
                st.markdown("---")
st.divider()

# ---- Data Health & Diagnostics ----
st.header("Data Health & Diagnostics")

with st.expander("Connection Diagnostics"):
    try:
        acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
        st.write("Service account email:", acct)
        st.write("Spreadsheet ID in use:", SPREADSHEET_ID)
        test_client = connect_to_gsheets()
        if test_client:
            ss = test_client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else test_client.open(GOOGLE_SHEET_NAME)
            st.write("Found worksheet tabs:", [w.title for w in ss.worksheets()])
    except Exception as e:
        st.write("Diagnostics error:", f"{type(e).__name__}: {e}")

# ---- Normalization Helpers for Cross-Check ----
def _strip_diacritics(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()

def _norm_text_for_compare(s: str, is_title: bool = True) -> str:
    s = _strip_diacritics(str(s))
    if is_title:
        s = re.split(r"[:(\\[]", s, 1)[0]
        s = re.sub(r"\b(a|an|the)\b\s+", "", s, flags=re.I)
    else:
        s = keep_primary_author(s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()

@st.cache_data(ttl=86400)
def _find_isbn_by_ta(title: str, author: str) -> str | None:
    """Fallback to find an ISBN using title and author search on Google Books."""
    if not title or not author:
        return None
    try:
        q = f'intitle:"{title}" inauthor:"{author}"'
        params = {"q": q, "printType": "books", "maxResults": 1}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=12, headers=UA)
        if r.ok:
            items = r.json().get("items", [])
            if not items:
                return None
            for ident in items[0].get("volumeInfo", {}).get("industryIdentifiers", []):
                if ident.get("type") in ("ISBN_13", "ISBN_10"):
                    return ident.get("identifier")
    except Exception:
        return None
    return None

# ---- Interactive Data Deep-Clean & Repair ----
with st.expander("🛠️ Data Deep-Clean & Repair (Library)", expanded=False):
    st.info("This tool checks each book against online sources to find and fix errors like misspelled titles, missing covers, and more.", icon="ℹ️")
    if st.button("Start Deep-Clean & Repair", key="start_deep_clean"):
        lib = load_data("Library")
        if lib.empty:
            st.info("Library is empty. Nothing to clean.")
        else:
            issues_found = 0
            progress_bar = st.progress(0, "Starting scan...")
            total_rows = len(lib)

            with st.container(): # Use a container to hold dynamic results
                for i, row in lib.iterrows():
                    progress_bar.progress((i + 1) / total_rows, f"Scanning row {i+1}/{total_rows}: {row.get('Title', '...')}")
                    sheet_data = row.to_dict()
                    sheet_row_index = i + 2

                    isbn = _normalize_isbn(str(sheet_data.get("ISBN", "")))
                    if not isbn: # If no ISBN in sheet, try to find one online
                        isbn = _find_isbn_by_ta(sheet_data.get("Title"), sheet_data.get("Author"))
                    if not isbn:
                        continue # Skip if we can't identify the book by ISBN

                    canonical_data = get_book_metadata(isbn)
                    if not canonical_data.get("Title"):
                        continue

                    mismatches = {}
                    if SequenceMatcher(None, _norm_text_for_compare(sheet_data.get("Title"), True), _norm_text_for_compare(canonical_data.get("Title"), True)).ratio() < 0.95:
                        mismatches['Title'] = {'sheet': sheet_data.get("Title"), 'canonical': canonical_data.get("Title")}
                    if SequenceMatcher(None, _norm_text_for_compare(sheet_data.get("Author"), False), _norm_text_for_compare(canonical_data.get("Author"), False)).ratio() < 0.95:
                        mismatches['Author'] = {'sheet': sheet_data.get("Author"), 'canonical': canonical_data.get("Author")}

                    for field in ["Description", "Language", "Thumbnail"]:
                        sheet_val = str(sheet_data.get(field, "")).strip()
                        is_sheet_empty = not sheet_val or sheet_val.lower() == 'nan'
                        if is_sheet_empty and str(canonical_data.get(field, "")).strip():
                            mismatches[field] = {'sheet': '(empty)', 'canonical': canonical_data.get(field)}

                    if mismatches:
                        issues_found += 1
                        st.markdown("---")
                        st.error(f"**Issue found in Row {sheet_row_index}: *{sheet_data.get('Title')}***")
                        for field, values in mismatches.items():
                            c1, c2 = st.columns(2)
                            val_sheet = str(values['sheet'])
                            val_canon = str(values['canonical'])
                            if len(val_sheet) > 150: val_sheet = val_sheet[:150] + '...'
                            if len(val_canon) > 150: val_canon = val_canon[:150] + '...'
                            c1.markdown(f"**{field} (Current):**\n`{val_sheet}`")
                            c2.markdown(f"**{field} (Suggestion):**\n`{val_canon}`")

                        update_key = f"update_row_{sheet_row_index}"
                        if st.button("✅ Accept & Update Row", key=update_key):
                            corrected_record = sheet_data.copy()
                            # Ensure the canonical ISBN is used if we found one
                            corrected_record['ISBN'] = isbn
                            for field, values in mismatches.items():
                                corrected_record[field] = values['canonical']
                            try:
                                update_gsheet_row("Library", sheet_row_index, corrected_record)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Update failed: {e}")

            progress_bar.empty()
            if issues_found == 0:
                st.success("✨ Deep-clean complete. No major issues found!")
            else:
                st.info(f"Scan complete. Found {issues_found} entries with potential issues to correct.")
