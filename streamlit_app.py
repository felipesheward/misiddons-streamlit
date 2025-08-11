#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Form + Scanner + Recommendations)
"""

from __future__ import annotations
import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
from PIL import Image
import re
from gspread.exceptions import APIError, WorksheetNotFound, SpreadsheetNotFound

# Optional barcode support
try:
    from pyzbar.pyzbar import decode as zbar_decode
except Exception:
    zbar_decode = None

# ---------- CONFIG ----------
DEFAULT_SHEET_ID = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"

def _extract_sheet_id(s: str) -> str:
    s = (s or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", s)
    return m.group(1) if m else s

RAW_SPREADSHEET_ID = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)
SPREADSHEET_ID = _extract_sheet_id(RAW_SPREADSHEET_ID)
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")
GOOGLE_BOOKS_KEY = st.secrets.get("google_books_api_key", None)

st.set_page_config(page_title="Misiddons Book Database", layout="wide")

# ---------- Google Sheets helpers ----------
@st.cache_resource
def connect_to_gsheets():
    if "gcp_service_account" not in st.secrets:
        st.error("gcp_service_account not found in secrets.")
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
        ss = client_local.open_by_key(SPREADSHEET_ID)
        ws = None
        try:
            ws = ss.worksheet(worksheet)
        except WorksheetNotFound:
            names = [w.title for w in ss.worksheets()]
            norm = {n.strip().casefold(): n for n in names}
            if worksheet.strip().casefold() in norm:
                ws = ss.worksheet(norm[worksheet.strip().casefold()])
            else:
                raise
        df = pd.DataFrame(ws.get_all_records())
        return df.dropna(how="all")
    except SpreadsheetNotFound:
        acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
        st.error(
            f"Spreadsheet not found.\nID: `{SPREADSHEET_ID}`\n"
            f"Share your sheet with `{acct}` as **Editor**."
        )
        return pd.DataFrame()
    except APIError as e:
        code = getattr(getattr(e, 'response', None), 'status_code', 'unknown')
        st.error(f"Google Sheets API error ({code}).")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error loading '{worksheet}': {type(e).__name__}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def _get_ws(tab: str):
    client = connect_to_gsheets()
    if not client:
        return None
    try:
        ss = client.open_by_key(SPREADSHEET_ID)
        return ss.worksheet(tab)
    except SpreadsheetNotFound:
        acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
        st.error(f"Spreadsheet not found. Share with `{acct}`.")
        return None

# ---------- Sheet write helpers ----------
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
            st.info(f"Already in {tab} (same ISBN).")
            return
        if inc_ta in existing_ta:
            st.info(f"Already in {tab} (same Title+Author).")
            return
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
    try:
        params = {"q": f"isbn:{isbn}", "maxResults": 1}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=12)
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            return {}
        info = items[0].get("volumeInfo", {})
        authors = info.get("authors", [])
        author = authors[0] if authors else ""
        thumbs = info.get("imageLinks") or {}
        thumb = thumbs.get("thumbnail", "")
        return {
            "ISBN": isbn,
            "Title": info.get("title", ""),
            "Author": author,
            "Genre": ", ".join(info.get("categories", [])),
            "Language": (info.get("language") or "").upper(),
            "Thumbnail": thumb,
            "Description": info.get("description", ""),
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
            timeout=12,
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}") or {}
        authors_list = data.get("authors", [])
        author = authors_list[0].get("name", "") if authors_list else ""
        cover = (data.get("cover") or {}).get("large", "")
        desc = data.get("description", "")
        if isinstance(desc, dict):
            desc = desc.get("value", "")
        return {
            "ISBN": isbn,
            "Title": data.get("title", ""),
            "Author": author,
            "Genre": ", ".join([s.get("name", "") for s in data.get("subjects", [])]),
            "Language": "",
            "Thumbnail": cover,
            "Description": desc,
            "PublishedDate": data.get("publish_date", ""),
        }
    except Exception:
        return {}

def get_book_metadata(isbn: str) -> dict:
    g = get_book_details_google(isbn)
    o = get_book_details_openlibrary(isbn)
    meta = {**o, **g}  # Google overrides OpenLibrary if present
    for k in ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
        meta.setdefault(k, "")
    return meta

@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list:
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"inauthor:{quote(author)}", "maxResults": 10},
            timeout=12,
        )
        if r.ok:
            return r.json().get("items", [])
    except Exception:
        pass
    return []

# ---------- UI ----------
st.title("Misiddons Book Database")

tabs = st.tabs(["Library", "Wishlist", "Recommendations"])

with tabs[0]:
    st.header("My Library")
    library_df = load_data("Library")
    if not library_df.empty:
        st.dataframe(library_df)
    else:
        st.info("Library empty.")

with tabs[1]:
    st.header("My Wishlist")
    wishlist_df = load_data("Wishlist")
    if not wishlist_df.empty:
        st.dataframe(wishlist_df)
    else:
        st.info("Wishlist empty.")

with tabs[2]:
    st.header("Recommendations")
    library_df = load_data("Library")
    wishlist_df = load_data("Wishlist")
    owned_titles = set(library_df["Title"].dropna().str.lower()) | set(wishlist_df["Title"].dropna().str.lower())
    if not library_df.empty:
        authors = library_df["Author"].dropna().unique()
        author = st.selectbox("Select author:", authors)
        if author:
            recs = get_recommendations_by_author(author)
            shown = 0
            for item in recs:
                vi = item.get("volumeInfo", {})
                if vi.get("title", "").lower() in owned_titles:
                    continue
                cols = st.columns([1, 4])
                with cols[0]:
                    if "imageLinks" in vi:
                        st.image(vi["imageLinks"].get("thumbnail", ""), width=100)
                with cols[1]:
                    st.subheader(vi.get("title", "No Title"))
                    st.write(f"**Author(s):** {', '.join(vi.get('authors', []))}")
                    st.write(f"**Published:** {vi.get('publishedDate', 'N/A')}")
                    st.caption(vi.get("description", "No description"))
                    meta = get_book_metadata(vi.get("industryIdentifiers", [{}])[0].get("identifier", ""))
                    a1, a2 = st.columns(2)
                    with a1:
                        if st.button(f"Add to Library {shown}", key=f"lib{shown}"):
                            append_record("Library", meta)
                            st.success("Added to Library ✔")
                    with a2:
                        if st.button(f"Add to Wishlist {shown}", key=f"wl{shown}"):
                            append_record("Wishlist", meta)
                            st.success("Added to Wishlist ✔")
                st.markdown("---")
                shown += 1
                if shown >= 4:
                    break
    else:
        st.info("Add books to your Library to get recommendations.")
