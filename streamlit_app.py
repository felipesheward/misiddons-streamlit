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

SPREADSHEET_ID = st.secrets.get("google_sheet_id")
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name")

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
    """Fetch a worksheet into a DataFrame. Avoid passing unhashable client into cache."""
    client_local = connect_to_gsheets()
    if not client_local:
        return pd.DataFrame()
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
        df = pd.DataFrame(ws.get_all_records())
        return df.dropna(how="all")
    except WorksheetNotFound:
        st.error(f"Worksheet '{worksheet}' not found. Available tabs: {[w.title for w in ss.worksheets()]} ")
        return pd.DataFrame()
    except APIError as e:
        code = getattr(getattr(e, 'response', None), 'status_code', 'unknown')
        if code == 404:
            st.error("Google Sheets API returned 404. Check the spreadsheet sharing/ID.")
        else:
            st.error(f"Google Sheets API error ({code}). {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error loading '{worksheet}': {e}")
        return pd.DataFrame()

@st.cache_data(ttl=86400)
def get_book_details(isbn: str) -> dict:
    if not isbn or len(isbn) < 10:
        return {}
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

if client and library_df.empty and wishlist_df.empty:
    st.warning("No data loaded. Check your sheet ID/name and permissions.")
    st.stop()

st.title("Misiddons Book Database")

# Add book form
with st.form("entry_form"):
    cols = st.columns(5)
    title = cols[0].text_input("Title")
    author = cols[1].text_input("Author")
    isbn = cols[2].text_input("ISBN")
    date_read = cols[3].text_input("Date Read")
    choice = cols[4].radio("Add to:", ["Library", "Wishlist"], horizontal=True)
    if st.form_submit_button("Add Book"):
        if title and author:
            try:
                ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
                ws = ss.worksheet(choice)
                ws.append_row([title, author, isbn, date_read])
                st.success(f"Added '{title}' to {choice}.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Failed to add book: {e}")
        else:
            st.warning("Enter both title and author.")

# Barcode scanner
if zbar_decode:
    st.header("Scan Barcode")
    up = st.file_uploader("Upload image", type=["png","jpg","jpeg"])
    if up:
        img = Image.open(up)
        codes = zbar_decode(img)
        if codes:
            isbn_bc = codes[0].data.decode()
            st.info(f"ISBN: {isbn_bc}")
            info = get_book_details(isbn_bc).get("volumeInfo", {})
            st.text_input("Title", info.get("title",""), key="btitle")
            st.text_input("Author", ", ".join(info.get("authors",[])), key="bauthor")
            st.text_input("ISBN", isbn_bc, key="bisbn")
        else:
            st.warning("No barcode found.")

st.divider()

# ---- Diagnostics (safe to show) ----
with st.expander("Diagnostics – help me if it still fails"):
    try:
        acct = st.secrets["gcp_service_account"].get("client_email", "(missing)") if "gcp_service_account" in st.secrets else "(no secrets found)"
        st.write("Service account email:", acct)
        st.write("Spreadsheet ID:", SPREADSHEET_ID or "(not set)")
        if client:
            try:
                ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
                st.write("Found worksheet tabs:", [w.title for w in ss.worksheets()])
            except Exception as e:
                st.write("Open spreadsheet error:", str(e))
    except Exception as e:
        st.write("Diagnostics error:", str(e))

# Tabs
tabs = st.tabs(["Library","Wishlist","Recommendations"])
with tabs[0]:
    st.header("My Library")
    st.dataframe(library_df) if not library_df.empty else st.info("Library is empty.")
with tabs[1]:
    st.header("My Wishlist")
    st.dataframe(wishlist_df) if not wishlist_df.empty else st.info("Wishlist is empty.")
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
