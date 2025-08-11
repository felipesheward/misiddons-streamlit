#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app
- Add books manually via form
- Scan barcodes to auto-fill
- Fetch metadata from Google Books + OpenLibrary
- Show 4 recommendations not already in Library/Wishlist
- No translators in Author field
"""
from __future__ import annotations
import re
import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
from PIL import Image
from gspread.exceptions import APIError, WorksheetNotFound, SpreadsheetNotFound

# Optional barcode support
try:
    from pyzbar.pyzbar import decode as zbar_decode
except Exception:
    zbar_decode = None

# ---------- CONFIG ----------
def _extract_sheet_id(s: str) -> str:
    s = (s or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", s)
    return m.group(1) if m else s

DEFAULT_SHEET_ID = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"
RAW_SPREADSHEET_ID = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)
SPREADSHEET_ID = _extract_sheet_id(RAW_SPREADSHEET_ID)
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")
GOOGLE_BOOKS_KEY = st.secrets.get("google_books_api_key", None)

st.set_page_config(page_title="Misiddons Book Database", layout="wide")

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
    client_local = connect_to_gsheets()
    if not client_local:
        return pd.DataFrame()
    try:
        ss = client_local.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client_local.open(GOOGLE_SHEET_NAME)
        ws = ss.worksheet(worksheet)
        df = pd.DataFrame(ws.get_all_records())
        return df.dropna(how="all")
    except SpreadsheetNotFound:
        acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
        st.error(f"Spreadsheet not found or not shared.\n\nID: `{SPREADSHEET_ID}`\nShare with: `{acct}` (Editor)")
        return pd.DataFrame()
    except WorksheetNotFound:
        st.error(f"Worksheet '{worksheet}' not found in spreadsheet.")
        return pd.DataFrame()
    except APIError as e:
        st.error(f"Google Sheets API error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error loading '{worksheet}': {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def _get_ws(tab: str):
    try:
        client = connect_to_gsheets()
        ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
        return ss.worksheet(tab)
    except SpreadsheetNotFound:
        acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
        st.error(f"Spreadsheet not found or not shared.\nShare with: `{acct}`")
    except WorksheetNotFound:
        st.error(f"Worksheet '{tab}' not found.")
    return None

# ---------- Write helpers ----------
EXACT_HEADERS = [
    "ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate","Date Read"
]

def _normalize_isbn(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s).replace("'", "") if ch.isdigit())

def append_record(tab: str, record: dict) -> None:
    try:
        ws = _get_ws(tab)
        if not ws:
            return
        headers = [h.strip() for h in ws.row_values(1)] or EXACT_HEADERS[:]
        extras = [h for h in headers if h not in EXACT_HEADERS]
        headers = EXACT_HEADERS[:] + extras
        ws.update('A1', [headers])
        values = ws.get_all_values()
        existing_isbns, existing_ta = set(), set()
        i_isbn, i_title, i_author = headers.index("ISBN"), headers.index("Title"), headers.index("Author")
        for r in values[1:]:
            if len(r) > i_isbn:
                norm = _normalize_isbn(r[i_isbn])
                if norm:
                    existing_isbns.add(norm)
            if len(r) > max(i_title, i_author):
                existing_ta.add(((r[i_title] or "").strip().lower(), (r[i_author] or "").strip().lower()))
        inc_isbn_norm = _normalize_isbn(record.get("ISBN", ""))
        inc_ta = (record.get("Title", "").strip().lower(), record.get("Author", "").strip().lower())
        if inc_isbn_norm in existing_isbns or inc_ta in existing_ta:
            st.info(f"'{record.get('Title','(unknown)')}' already in {tab}. Skipped.")
            return
        if record.get("ISBN") and str(record["ISBN"]).isdigit():
            record["ISBN"] = "'" + str(record["ISBN"]).strip()
        row = [record.get(h, "") for h in headers]
        ws.append_row(row, value_input_option="RAW")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")

# ---------- Metadata fetch ----------
@st.cache_data(ttl=86400)
def get_book_details_google(isbn: str) -> dict:
    if not isbn:
        return {}
    try:
        params = {"q": f"isbn:{isbn}", "printType": "books", "maxResults": 1}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=12)
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            return {}
        info = items[0].get("volumeInfo", {})
        authors = info.get("authors", [])
        return {
            "ISBN": isbn,
            "Title": info.get("title", ""),
            "Author": authors[0] if authors else "",
            "Genre": ", ".join(info.get("categories", [])),
            "Language": (info.get("language") or "").upper(),
            "Thumbnail": info.get("imageLinks", {}).get("thumbnail", ""),
            "Description": info.get("description", "").strip(),
            "Rating": str(info.get("averageRating", "")),
            "PublishedDate": info.get("publishedDate", ""),
        }
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def get_book_details_openlibrary(isbn: str) -> dict:
    try:
        r = requests.get(
            "https://openlibrary.org/api/books",
            params={"bibkeys": f"ISBN:{isbn}", "jscmd": "data", "format": "json"},
            timeout=12
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}") or {}
        authors_list = data.get("authors", [])
        authors = authors_list[0].get("name", "") if authors_list else ""
        subjects = ", ".join(s.get("name", "") for s in data.get("subjects", []))
        cover = (data.get("cover") or {}).get("large") or ""
        desc = data.get("description", "")
        if isinstance(desc, dict):
            desc = desc.get("value", "")
        lang = ""
        try:
            lang = (data.get("languages", [{}])[0].get("key", "").split("/")[-1] or "").upper()
        except Exception:
            pass
        return {
            "ISBN": isbn,
            "Title": data.get("title", ""),
            "Author": authors,  # ✅ No translators appended
            "Genre": subjects,
            "Language": lang,
            "Thumbnail": cover,
            "Description": (desc or "").strip(),
            "PublishedDate": data.get("publish_date", ""),
        }
    except Exception:
        return {}

def get_book_metadata(isbn: str) -> dict:
    google_meta = get_book_details_google(isbn)
    openlibrary_meta = get_book_details_openlibrary(isbn)
    meta = {**openlibrary_meta, **google_meta}  # Prefer Google fields if available
    for k in ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
        meta.setdefault(k, "")
    return meta

@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list:
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"inauthor:{quote(author)}", "maxResults": 10},
            timeout=12
        )
        if r.ok:
            return r.json().get("items", [])
    except Exception:
        pass
    return []

# ---------- UI ----------
st.title("Misiddons Book Database")

# --- Form ---
with st.expander("✍️ Add a New Book", expanded=False):
    with st.form("entry_form"):
        cols = st.columns(5)
        title = cols[0].text_input("Title", value=st.session_state.get("scan_title", ""))
        author = cols[1].text_input("Author", value=st.session_state.get("scan_author", ""))
        isbn = cols[2].text_input("ISBN", value=st.session_state.get("scan_isbn", ""))
        date_read = cols[3].text_input("Date Read", placeholder="YYYY/MM/DD")
        choice = cols[4].radio("Add to:", ["Library", "Wishlist"], horizontal=True)
        if st.form_submit_button("Add Book") and title and author:
            rec = {"ISBN": isbn, "Title": title, "Author": author, "Date Read": date_read}
            append_record(choice, rec)
            st.success(f"Added '{title}' to {choice}.")
            st.session_state.clear()

# --- Scanner ---
if zbar_decode:
    with st.expander("📷 Scan Barcode from Photo"):
        up = st.file_uploader("Upload barcode photo", type=["png","jpg","jpeg"])
        if up:
            img = Image.open(up).convert("RGB")
            codes = zbar_decode(img)
            if not codes:
                st.warning("No barcode found.")
            else:
                raw = codes[0].data.decode(errors="ignore")
                isbn_bc = "".join(ch for ch in raw if ch.isdigit())[-13:]
                meta = get_book_metadata(isbn_bc)
                st.session_state.update(scan_isbn=meta["ISBN"], scan_title=meta["Title"], scan_author=meta["Author"])
                st.json(meta)

# --- Tabs ---
tabs = st.tabs(["Library", "Wishlist", "Recommendations"])
with tabs[0]:
    st.header("My Library")
    lib_df = load_data("Library")
    st.dataframe(lib_df if not lib_df.empty else pd.DataFrame(), use_container_width=True)

with tabs[1]:
    st.header("My Wishlist")
    wl_df = load_data("Wishlist")
    st.dataframe(wl_df if not wl_df.empty else pd.DataFrame(), use_container_width=True)

with tabs[2]:
    st.header("Recommendations")
    combined_authors = pd.concat([lib_df["Author"], wl_df["Author"]], ignore_index=True).dropna().unique()
    owned_titles = set(pd.concat([lib_df["Title"], wl_df["Title"]], ignore_index=True).dropna().str.lower())
    rec_books = []
    for auth in combined_authors:
        rec_books.extend(get_recommendations_by_author(auth))
    shown = 0
    for item in rec_books:
        vi = item.get("volumeInfo", {})
        title = vi.get("title", "").lower()
        if title in owned_titles:
            continue
        cols = st.columns([1,4])
        with cols[0]:
            if "imageLinks" in vi:
                st.image(vi["imageLinks"].get("thumbnail", ""), width=100)
        with cols[1]:
            st.subheader(vi.get("title", "No Title"))
            st.write(f"**Author(s):** {', '.join(vi.get('authors', []))}")
            st.write(f"**Published:** {vi.get('publishedDate', 'N/A')}")
            if st.button("Add to Library", key=f"rec_lib_{shown}"):
                append_record("Library", get_book_metadata(vi.get("industryIdentifiers", [{}])[0].get("identifier", "")))
            if st.button("Add to Wishlist", key=f"rec_wl_{shown}"):
                append_record("Wishlist", get_book_metadata(vi.get("industryIdentifiers", [{}])[0].get("identifier", "")))
        shown += 1
        if shown >= 4:
            break
