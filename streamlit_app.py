#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app
"""
import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
from PIL import Image
from gspread.exceptions import APIError, WorksheetNotFound

# ---------- CONFIGURATION ----------
# Set your Google Sheets info via Streamlit secrets:
#   [gcp_service_account]
#   type = "service_account"
#   project_id = "..."
#   private_key_id = "..."
#   private_key = "-----BEGIN PRIVATE KEY-----\n..."
#   client_email = "..."
#   client_id = "..."
#   auth_uri = "..."
#   token_uri = "..."
#   auth_provider_x509_cert_url = "..."
#   client_x509_cert_url = "..."
#   universe_domain = "..."
#
#   google_sheet_id = "<YOUR_SPREADSHEET_ID>"
#   google_sheet_name = "database"

DEFAULT_SHEET_ID = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"
SPREADSHEET_ID = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")

# Optional barcode support
try:
    from pyzbar.pyzbar import decode as zbar_decode
except ImportError:
    zbar_decode = None

st.set_page_config(page_title="Misiddons Book Database", layout="wide")

@st.cache_resource
def connect_to_gsheets():
    """Connect to Google Sheets using service account info from Streamlit secrets."""
    if "gcp_service_account" not in st.secrets:
        st.error("gcp_service_account block not found in Streamlit secrets.")
        return None
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly"
        ]
        )
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Failed to authorize with provided service account: {e}")
        return None

@st.cache_data(ttl=60)
def load_data(worksheet: str) -> pd.DataFrame:
    """Fetch a worksheet into a DataFrame. Avoid passing unhashable client into cache.
    Falls back to get_all_values() if get_all_records() fails.
    """
    client_local = connect_to_gsheets()
    if not client_local:
        return pd.DataFrame()
    ss = None
    try:
        ss = client_local.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client_local.open(GOOGLE_SHEET_NAME)
        # Try exact, then forgiving match (strip+casefold)
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
            # Primary path
            records = ws.get_all_records()
            df = pd.DataFrame(records)
            return df.dropna(how="all")
        except Exception as e1:
            # Fallback path – raw values with first row as header
            vals = ws.get_all_values()
            if not vals:
                return pd.DataFrame()
            header, *rows = vals
            df = pd.DataFrame(rows, columns=header)
            return df.dropna(how="all")
    except WorksheetNotFound:
        try:
            tabs = [w.title for w in ss.worksheets()] if ss else []
        except Exception:
            tabs = []
        st.error(f"Worksheet '{worksheet}' not found. Available tabs: {tabs}")
        return pd.DataFrame()
    except APIError as e:
        code = getattr(getattr(e, 'response', None), 'status_code', 'unknown')
        st.error(f"Google Sheets API error while loading '{worksheet}' (HTTP {code}). If 404/403, re‑share the sheet with the service account and verify the ID.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error loading '{worksheet}': {type(e).__name__}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=86400)
def get_book_details(isbn: str) -> dict:
    """Primary metadata fetcher (Google Books) with a consistent shape."""
    if not isbn:
        return {}
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"isbn:{isbn}", "printType": "books", "maxResults": 1},
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            return {}
        info = items[0].get("volumeInfo", {})
        desc = info.get("description") or items[0].get("searchInfo", {}).get("textSnippet")
        thumbs = (info.get("imageLinks") or {})
        thumb = thumbs.get("thumbnail") or thumbs.get("smallThumbnail")
        cats = info.get("categories") or []
        # Normalize to our sheet headers
        return {
            "ISBN": isbn,
            "Title": info.get("title", ""),
            "Author": ", ".join(info.get("authors", [])),
            "Genre": ", ".join(cats) if cats else "",
            "Language": (info.get("language") or "").upper(),
            "Thumbnail": thumb or "",
            "Description": desc or "",
        }
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def get_book_details_fallback(isbn: str) -> dict:
    """Fallback metadata via Open Library (best-effort)."""
    try:
        r = requests.get(
            "https://openlibrary.org/api/books",
            params={"bibkeys": f"ISBN:{isbn}", "jscmd": "data", "format": "json"},
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}")
        if not data:
            return {}
        authors = ", ".join([a.get("name", "") for a in data.get("authors", []) if a])
        subjects = ", ".join([s.get("name", "") for s in data.get("subjects", []) if s])
        cover = (data.get("cover") or {}).get("medium") or (data.get("cover") or {}).get("large") or ""
        return {
            "ISBN": isbn,
            "Title": data.get("title", ""),
            "Author": authors,
            "Genre": subjects,
            "Language": (data.get("languages", [{}])[0].get("key", "").split("/")[-1] or "").upper(),
            "Thumbnail": cover,
            "Description": data.get("notes", "") if isinstance(data.get("notes"), str) else "",
        }
    except Exception:
        return {}

def get_book_metadata(isbn: str) -> dict:
    """Try Google first, then OpenLibrary. Returns normalized dict or {}."""
    meta = get_book_details(isbn)
    if meta:
        return meta
    return get_book_details_fallback(isbn)

    r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}")
    if r.ok:
        data = r.json()
        return data.get("items", [{}])[0]
    return {}

@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list:
    if not author:
        return []
    r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=inauthor:{quote(author)}")
    if r.ok:
        return r.json().get("items", [])
    return []

# Main
client = connect_to_gsheets()
library_df = load_data("Library")
wishlist_df = load_data("Wishlist")

if library_df.empty and wishlist_df.empty:
    st.warning("No data loaded. Check your sheet ID/name, tab names, and sharing permissions.")

st.title("Misiddons Book Database")

# Add book form
with st.form("entry_form"):
    # Detect current headers so we append in the right order
    current_headers = []
    try:
        ss_hdr = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
        ws_hdr = ss_hdr.worksheet("Library")
        current_headers = ws_hdr.row_values(1)
    except Exception:
        current_headers = ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating"]

    cols = st.columns(5)
    title = cols[0].text_input("Title", value=st.session_state.get("scan_title", ""))
    author = cols[1].text_input("Author", value=st.session_state.get("scan_author", ""))
    isbn = cols[2].text_input("ISBN (Optional)", value=st.session_state.get("scan_isbn", ""))
    date_read = cols[3].text_input("Date Read", placeholder="YYYY/MM/DD")
    choice = cols[4].radio("Add to:", ["Library", "Wishlist"], horizontal=True)

    def _append_record(target_ws, record: dict):
        # Build a row matching current headers; unknown headers become empty
        keymap = {h.lower(): h for h in current_headers}
        # include Date Read if user keeps the older sheet
        record = {**record, "Date Read": date_read}
        row = [record.get(keymap.get(h.lower(), h), record.get(h, "")) for h in current_headers]
        target_ws.append_row(row, value_input_option="USER_ENTERED")

    if st.form_submit_button("Add Book"):
        if title and author:
            try:
                ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
                ws = ss.worksheet(choice)
                rec = {"ISBN": isbn, "Title": title, "Author": author}
                _append_record(ws, rec)
                st.success(f"Added '{title}' to {choice}.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Failed to add book: {e}")
        else:
            st.warning("Enter both title and author.")

# Barcode scanner
if zbar_decode:
    with st.expander("📷 Scan Barcode"):
        up = st.file_uploader("Upload an image with a barcode", type=["png","jpg","jpeg"])
        if up:
            img = Image.open(up)
            codes = zbar_decode(img)
            if not codes:
                st.warning("No barcode found.")
            else:
                raw = codes[0].data.decode(errors="ignore")
                digits = "".join(ch for ch in raw if ch.isdigit())
                # Heuristic: many book EANs are 13 digits starting with 978/979
                isbn_bc = digits[-13:] if len(digits) >= 13 else digits
                st.info(f"Detected code: {raw} → Using ISBN: {isbn_bc}")

                meta = get_book_metadata(isbn_bc)
                if not meta:
                    st.error("Couldn't fetch book details from Google/OpenLibrary.")
                else:
                    # Preview card
                    cols = st.columns([1,3])
                    with cols[0]:
                        if meta.get("Thumbnail"):
                            st.image(meta["Thumbnail"], caption=meta.get("Title",""))
                    with cols[1]:
                        st.subheader(meta.get("Title","Unknown Title"))
                        st.write(f"**Author:** {meta.get('Author','Unknown')}")
                        if meta.get("Genre"):
                            st.write(f"**Genre:** {meta.get('Genre')}")
                        if meta.get("Language"):
                            st.write(f"**Language:** {meta.get('Language')}")
                        if meta.get("Description"):
                            st.caption(meta["Description"][:600] + ("…" if len(meta["Description"])>600 else ""))

                    # Add buttons
                    add_cols = st.columns([1,1])
                    with add_cols[0]:
                        if st.button("➕ Add to Library", key="add_scan_lib"):
                            try:
                                ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
                                ws = ss.worksheet("Library")
                                headers = ws.row_values(1)
                                keymap = {h.lower(): h for h in headers}
                                row = [meta.get(keymap.get(h.lower(), h), meta.get(h, "")) for h in headers]
                                ws.append_row(row, value_input_option="USER_ENTERED")
                                st.success("Book added to Library.")
                                st.cache_data.clear()
                            except Exception as e:
                                st.error(f"Failed to add to Library: {e}")
                    with add_cols[1]:
                        if st.button("📝 Fill form above", key="fill_form_from_scan"):
                            st.session_state["scan_title"] = meta.get("Title", "")
                            st.session_state["scan_author"] = meta.get("Author", "")
                            st.session_state["scan_isbn"] = meta.get("ISBN", "")
                            st.success("Filled the form fields with scanned data.")
                            st.experimental_rerun()
else:
    st.info("Barcode scanning requires `pyzbar`/`zbar`. If unavailable in the environment, you can paste the ISBN manually above.")

st.divider()

# ---- Diagnostics (safe to show) ----
with st.expander("Diagnostics – help me if it still fails"):
    try:
        acct = st.secrets["gcp_service_account"].get("client_email", "(missing)") if "gcp_service_account" in st.secrets else "(no secrets found)"
        st.write("Service account email:", acct)
        st.write("Spreadsheet ID in use:", SPREADSHEET_ID)
        try:
            test_client = connect_to_gsheets()
            if test_client:
                ss = test_client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else test_client.open(GOOGLE_SHEET_NAME)
                st.write("Found worksheet tabs:", [w.title for w in ss.worksheets()])
        except Exception as e:
            st.write("Open spreadsheet error:", f"{type(e).__name__}: {e}")
            st.write("Tip: ensure this sheet is shared with:")
            st.code("book-app-connector@misiddons-book-databse.iam.gserviceaccount.com")
    except Exception as e:
        st.write("Diagnostics error:", f"{type(e).__name__}: {e}")

# Tabs
tabs = st.tabs(["Library","Wishlist","Recommendations"])
with tabs[0]:
    st.header("My Library")
    if not library_df.empty:
        st.dataframe(library_df, use_container_width=True)
    else:
        st.info("Library is empty.")
with tabs[1]:
    st.header("My Wishlist")
    if not wishlist_df.empty:
        st.dataframe(wishlist_df, use_container_width=True)
    else:
        st.info("Wishlist is empty.")
with tabs[2]:
    st.header("Recommendations")
    if not library_df.empty:
        auths = library_df["Author"].dropna().unique()
        sel = st.selectbox("Authors:", auths)
        for item in get_recommendations_by_author(sel):
            vi = item.get("volumeInfo", {})
            st.subheader(vi.get("title",""))
            st.write(f"**Authors:** {', '.join(vi.get('authors',[]))}")
            thumb = vi.get("imageLinks",{}).get("thumbnail")
            if thumb: st.image(thumb)
            st.markdown("---")
    else:
        st.info("Read some books to see recommendations.")
