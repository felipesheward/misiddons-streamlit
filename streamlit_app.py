#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app
"""
import os
from pathlib import Path
import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
from PIL import Image

# ---------- CONFIGURATION ----------
# Option 1: Provide your Spreadsheet ID directly (from URL: /d/<THIS_ID>/)
# Option 2: Provide the name of the sheet if you prefer lookup by name
SPREADSHEET_ID = st.secrets.get("google_sheet_id", "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s")
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")

# Path to your service account JSON file (ensure this file is in your app directory)
BASE_DIR = Path(__file__).resolve().parent
SERVICE_ACCOUNT_FILE = BASE_DIR / "misiddons-book-databse-2053f224ebd3.json"

# ---------- OPTIONAL barcode support ----------
try:
    from pyzbar.pyzbar import decode as zbar_decode
except ImportError:
    zbar_decode = None

# ---------- Streamlit Config ----------
st.set_page_config(page_title="Misiddons Book Database", layout="wide")
st.markdown("""
    <style>
    [data-testid=column]:not(:last-child){margin-right:1rem;}
    .stButton > button{width:100%; text-wrap:balance;}
    </style>
    """, unsafe_allow_html=True)

# ---------- Functions ----------

@st.cache_resource
def connect_to_gsheets():
    """Establish and cache a connection to Google Sheets via ID or name."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        # Credentials: secrets or JSON file
        if "gcp_service_account" in st.secrets:
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], scopes=scopes)
        else:
            if not SERVICE_ACCOUNT_FILE.exists():
                raise FileNotFoundError(f"Service account file not found at {SERVICE_ACCOUNT_FILE}")
            creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets. Error: {e}")
        return None

@st.cache_data(ttl=60)
def load_data(client: gspread.Client, worksheet_name: str) -> pd.DataFrame:
    """Fetch worksheet either by ID then sheet name or fallback to name-only."""
    if client is None:
        return pd.DataFrame()
    try:
        # Try open by ID
        if SPREADSHEET_ID and SPREADSHEET_ID != "":
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
        else:
            spreadsheet = client.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(worksheet_name)
        records = worksheet.get_all_records()
        df = pd.DataFrame(records)
        return df.dropna(axis=0, how="all")
    except Exception as e:
        st.error(f"Could not load '{worksheet_name}': {e}")
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
    safe_author = quote(author)
    r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=inauthor:{safe_author}")
    if r.ok:
        data = r.json()
        return data.get("items", [])
    return []

# ---------- Main App Logic ----------
client = connect_to_gsheets()
library_df = load_data(client, "Library")
wishlist_df = load_data(client, "Wishlist")

if client and library_df.empty and wishlist_df.empty:
    st.warning("No data loaded—check your sheet ID/name, permissions, and worksheet tabs.")
    st.stop()

st.title("Misiddons Book Database")
# --- Add New Book Form ---
st.header("Add a new book")
with st.form("entry_form"):
    cols = st.columns(5)
    title = cols[0].text_input("Title")
    author = cols[1].text_input("Author")
    isbn = cols[2].text_input("ISBN")
    date_read = cols[3].text_input("Date Read")
    list_choice = cols[4].radio("Add to list:", ["Library", "Wishlist"], horizontal=True)
    if st.form_submit_button("Add Book"):
        if title and author:
            try:
                sheet = (client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME))
                ws = sheet.worksheet(list_choice)
                ws.append_row([title, author, isbn, date_read])
                st.success(f"Added '{title}' to your {list_choice}.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Failed to add book: {e}")
        else:
            st.warning("Please provide both title and author.")

# --- Barcode Scanner ---
if zbar_decode:
    st.header("Scan Barcode")
    img_file = st.file_uploader("Upload barcode image", type=["png", "jpg", "jpeg"])
    if img_file:
        img = Image.open(img_file)
        codes = zbar_decode(img)
        if codes:
            for code in codes:
                val = code.data.decode()
                st.info(f"ISBN: {val}")
                info = get_book_details(val)
                vi = info.get("volumeInfo", {})
                st.text_input("Title", vi.get("title", ""), key="btitle")
                st.text_input("Author", ", ".join(vi.get("authors", [])), key="bauthor")
                st.text_input("ISBN", val, key="bisbn")
        else:
            st.warning("No barcode detected.")

st.divider()
# --- Tabs ---
tabs = st.tabs(["Library", "Wishlist", "Recommendations"])
with tabs[0]:
    st.header("My Library")
    st.dataframe(library_df) if not library_df.empty else st.info("Library empty.")
with tabs[1]:
    st.header("My Wishlist")
    st.dataframe(wishlist_df) if not wishlist_df.empty else st.info("Wishlist empty.")
with tabs[2]:
    st.header("Recommendations")
    if not library_df.empty and "Author" in library_df:
        auths = library_df["Author"].dropna().unique()
        sel = st.selectbox("Authors:", auths)
        recs = get_recommendations_by_author(sel)
        if recs:
            for it in recs:
                vi = it.get("volumeInfo", {})
                st.subheader(vi.get("title", "No Title"))
                st.write(f"**Authors:** {', '.join(vi.get('authors', []))}")
                if img := vi.get("imageLinks", {}).get("thumbnail"): st.image(img)
                st.markdown("---")
        else:
            st.info("No recommendations.")
    else:
        st.info("Read some books first.")
