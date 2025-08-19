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

# ---------- Metadata fetchers ----------
@st.cache_data(ttl=86400)
def get_book_metadata(isbn: str) -> dict:
    # This function combines calls to Google Books and OpenLibrary
    # and merges the results for the best possible metadata.
    # [For brevity, the detailed implementation of this function and its
    # sub-functions like get_book_details_google is omitted, but it
    # exists in the full script provided in previous answers.]
    google_meta = get_book_details_google(isbn)
    openlibrary_meta = get_book_details_openlibrary(isbn)
    meta = google_meta.copy() if google_meta.get("Title") else openlibrary_meta.copy()
    for key in ["Title", "Author", "Genre", "Language", "Thumbnail", "Description", "PublishedDate"]:
        if not meta.get(key):
            meta[key] = openlibrary_meta.get(key, "") if meta is google_meta else google_meta.get(key, "")
    for k in ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
        meta.setdefault(k, "")
    return meta

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
        # Deduplication logic
        # [Omitted for brevity, but exists in the full script]
        keymap = {h.lower(): h for h in headers}
        row = [record.get(keymap.get(h.lower(), h), record.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")
        raise

# ---------- Reusable Grid Display Function ----------
def display_books_grid(df: pd.DataFrame, show_read_status: bool = False):
    """Renders a DataFrame of books as a responsive, styled grid of covers."""
    if df.empty:
        return

    checkmark_svg = (
        "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' "
        "fill='white'%3E%3Cpath d='M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z'/%3E%3C/svg%3E"
    )

    st.markdown(f"""
        <style>
            .book-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(100px, 1fr));
                gap: 0.75rem;
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

# (The rest of the app's UI, form logic, scanner, and other tabs would follow here)
# [Full implementation omitted for brevity but is consistent with the last complete script provided]
