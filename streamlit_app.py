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
    - NEW: Interactive Data Deep-Clean & Repair tool (v3 - Robust Caching)
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

UA = {"User-Agent": "misiddons/1.4"} # Version bump for new feature

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
    """Fetch a worksheet into a DataFrame, ensuring no NaN values."""
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
        # Use get_all_records which handles types better and use empty_value to prevent None.
        # Finally, use fillna("") to catch any remaining NaNs.
        data = ws.get_all_records(empty_value="", head=1)
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data).fillna("")
    except Exception as e:
        st.error(f"An unexpected error occurred while loading '{worksheet}': {e}")
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

def _normalize_isbn(s: str) -> str:
    if not s: return ""
    return "".join(ch for ch in str(s).replace("'", "") if ch.isdigit())

def keep_primary_author(author: str) -> str:
    s = (author or "").strip()
    if not s: return ""
    if s.count(',') >= 2: return s.split(',')[0].strip()
    if ' and ' in s: return s.split(' and ')[0].strip()
    if ' & ' in s: return s.split(' & ')[0].strip()
    return s

# --- Metadata Fetchers and other helpers ---
# [NOTE: The metadata fetching functions like get_book_metadata, get_book_details_google, etc. are unchanged]
@st.cache_data(ttl=86400)
def get_book_metadata(isbn: str) -> dict:
    if not isbn: return {}
    google_meta = get_book_details_google(isbn)
    openlibrary_meta = get_book_details_openlibrary(isbn)
    meta = google_meta.copy() if google_meta.get("Title") else openlibrary_meta.copy()
    for key in ["Title", "Author", "Genre", "Language", "Thumbnail", "Description", "PublishedDate"]:
        if not meta.get(key): meta[key] = openlibrary_meta.get(key, "") if meta is google_meta else google_meta.get(key, "")
    for k in EXACT_HEADERS: meta.setdefault(k, "")
    meta["Language"] = _pretty_lang(meta.get("Language", ""))
    if not meta.get("Thumbnail") and meta.get("ISBN"): meta["Thumbnail"] = f"https://covers.openlibrary.org/b/isbn/{meta['ISBN']}-L.jpg"
    meta["Author"] = keep_primary_author(meta.get("Author", ""))
    return meta

@st.cache_data(ttl=86400)
def get_book_details_google(isbn: str) -> dict:
    if not isbn: return {}
    try:
        # ... function content is unchanged ...
        params = {"q": f"isbn:{isbn}", "printType": "books", "maxResults": 1}
        if GOOGLE_BOOKS_KEY: params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=12, headers=UA)
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items: return {}
        info = items[0].get("volumeInfo", {})
        desc = info.get("description") or items[0].get("searchInfo", {}).get("textSnippet", "")
        thumbs = info.get("imageLinks") or {}
        thumb = thumbs.get("thumbnail") or thumbs.get("smallThumbnail") or ""
        if thumb.startswith("http://"): thumb = thumb.replace("http://", "https://")
        authors = info.get("authors") or []
        return {"ISBN": isbn, "Title": (info.get("title", "") or "").strip(), "Author": keep_primary_author(authors[0].strip()) if authors else "", "Genre": ", ".join(info.get("categories", [])), "Language": (info.get("language") or "").upper(), "Thumbnail": thumb, "Description": (desc or "").strip(), "PublishedDate": info.get("publishedDate", "")}
    except Exception: return {}

# [Other helper functions like get_book_details_openlibrary, get_recommendations_by_author, etc. are also unchanged]
# ...

def append_record(tab: str, record: dict) -> None:
    """Ensure headers, dedupe (ISBN or Title+Author), preserve ISBN as text, then append."""
    try:
        ws = _get_ws(tab)
        if not ws: raise RuntimeError("Worksheet not found")
        headers = [h.strip() for h in ws.row_values(1)]
        if not headers:
            headers = EXACT_HEADERS[:]
            ws.update('A1', [headers])
        else:
            extras = [h for h in headers if h not in EXACT_HEADERS]
            headers = EXACT_HEADERS[:] + extras
            if [h.strip() for h in ws.row_values(1)] != headers:
                ws.update('A1', [headers])
        all_df = pd.DataFrame(ws.get_all_records(empty_value=""))
        existing_isbns = set(all_df["ISBN"].astype(str).map(_normalize_isbn).dropna()) if "ISBN" in all_df else set()
        existing_ta = set(zip(all_df["Title"].str.strip().str.lower(), all_df["Author"].str.strip().str.lower())) if "Title" in all_df and "Author" in all_df else set()
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
        row = [record.get(h, "") for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")
        raise

def update_gsheet_row(tab: str, row_index: int, record: dict) -> None:
    """Finds a row by index and updates it with new data. More robust version."""
    try:
        ws = _get_ws(tab)
        if not ws: raise RuntimeError(f"Worksheet '{tab}' not found.")
        
        # Normalize headers from sheet to be safe
        headers = [h.strip() for h in ws.row_values(1)]
        if not headers:
            st.error(f"Cannot update row: worksheet '{tab}' has no headers.")
            return

        # Create a case-insensitive lookup for the new record data
        record_lookup = {str(k).lower(): v for k, v in record.items()}
        
        row_values = []
        for h in headers:
            # Find the value from the new record using a normalized key, otherwise keep the old value
            val = record_lookup.get(h.lower(), record.get(h))
            row_values.append(val if val is not None else "")

        # Specifically format ISBN as text for sheets
        if "ISBN" in headers:
            isbn_idx = headers.index("ISBN")
            isbn_val = str(row_values[isbn_idx])
            if isbn_val.isdigit():
                row_values[isbn_idx] = "'" + isbn_val
        
        ws.update(f'A{row_index}', [row_values], value_input_option="USER_ENTERED")
        
        # This is the most critical part: clear ALL caches to force a fresh read
        st.cache_data.clear()
        st.cache_resource.clear()

        st.success(f"Row {row_index} in '{tab}' was updated successfully in Google Sheets.")

    except Exception as e:
        st.error(f"Failed to update row {row_index} in '{tab}': {e}")
        raise


# ---------- UI (Forms and Tabs are mostly unchanged) ----------
st.title("Misiddons Book Database")

# [The code for the UI (forms, tabs, etc.) is unchanged, so it is omitted here for brevity]
# ...


# ---- Data Health & Diagnostics ----
st.divider()
st.header("Data Health & Diagnostics")

# [Connection Diagnostics expander is unchanged]
# ...

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
    if not title or not author: return None
    try:
        q = f'intitle:"{title}" inauthor:"{author}"'
        params = {"q": q, "printType": "books", "maxResults": 1, "key": GOOGLE_BOOKS_KEY} if GOOGLE_BOOKS_KEY else {"q": q, "printType": "books", "maxResults": 1}
        r = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=12, headers=UA)
        if r.ok:
            items = r.json().get("items", [])
            if not items: return None
            for ident in items[0].get("volumeInfo", {}).get("industryIdentifiers", []):
                if ident.get("type") in ("ISBN_13", "ISBN_10"):
                    return ident.get("identifier")
    except Exception:
        return None
    return None

# ---- Interactive Data Deep-Clean & Repair (v3) ----
with st.expander("🛠️ Data Deep-Clean & Repair (Library)", expanded=False):
    st.info("This tool checks each book against online sources to find and fix errors. It first uses the ISBN, then searches by Title+Author if the ISBN is missing.", icon="ℹ️")
    
    if st.button("Start Deep-Clean & Repair", key="start_deep_clean"):
        lib = load_data("Library")
        if lib.empty:
            st.info("Library is empty. Nothing to clean.")
        else:
            issues_found = 0
            progress_bar = st.progress(0, "Starting scan...")
            total_rows = len(lib)

            st.markdown("---")
            for i, row in lib.iterrows():
                progress_text = f"Scanning row {i+1}/{total_rows}: {row.get('Title', '...')}"
                progress_bar.progress((i + 1) / total_rows, progress_text)
                
                sheet_data = row.to_dict()
                sheet_row_index = i + 2

                isbn = _normalize_isbn(str(sheet_data.get("ISBN", "")))
                if not isbn:
                    isbn = _find_isbn_by_ta(sheet_data.get("Title"), sheet_data.get("Author"))
                if not isbn:
                    continue

                canonical_data = get_book_metadata(isbn)
                if not canonical_data.get("Title"):
                    continue

                mismatches = {}
                # Check for Title mismatch
                if SequenceMatcher(None, _norm_text_for_compare(sheet_data.get("Title"), True), _norm_text_for_compare(canonical_data.get("Title"), True)).ratio() < 0.95:
                    mismatches['Title'] = {'sheet': sheet_data.get("Title"), 'canonical': canonical_data.get("Title")}
                # Check for Author mismatch
                if SequenceMatcher(None, _norm_text_for_compare(sheet_data.get("Author"), False), _norm_text_for_compare(canonical_data.get("Author"), False)).ratio() < 0.95:
                    mismatches['Author'] = {'sheet': sheet_data.get("Author"), 'canonical': canonical_data.get("Author")}
                # Check for missing fields
                for field in ["Description", "Language", "Thumbnail"]:
                    sheet_val = str(sheet_data.get(field, "")).strip()
                    is_sheet_empty = not sheet_val or sheet_val.lower() == 'nan'
                    if is_sheet_empty and str(canonical_data.get(field, "")).strip():
                        mismatches[field] = {'sheet': '(empty)', 'canonical': canonical_data.get(field)}

                if mismatches:
                    issues_found += 1
                    st.error(f"**Issue found in Row {sheet_row_index}: *{sheet_data.get('Title')}***")
                    for field, values in mismatches.items():
                        c1, c2 = st.columns(2)
                        val_sheet = str(values['sheet']); val_canon = str(values['canonical'])
                        if len(val_sheet) > 150: val_sheet = val_sheet[:150] + '...'
                        if len(val_canon) > 150: val_canon = val_canon[:150] + '...'
                        c1.markdown(f"**{field} (Current):**\n`{val_sheet}`")
                        c2.markdown(f"**{field} (Suggestion):**\n`{val_canon}`")

                    update_key = f"update_row_{sheet_row_index}"
                    if st.button("✅ Accept & Update Row", key=update_key):
                        corrected_record = sheet_data.copy()
                        corrected_record['ISBN'] = isbn
                        for field, values in mismatches.items():
                            corrected_record[field] = values['canonical']
                        try:
                            update_gsheet_row("Library", sheet_row_index, corrected_record)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Update failed unexpectedly: {e}")
                    st.markdown("---")

            progress_bar.empty()
            if issues_found == 0:
                st.success("✨ Deep-clean complete. No issues found!")
            else:
                st.info(f"Scan complete. Found {issues_found} entries with potential issues to correct.")
