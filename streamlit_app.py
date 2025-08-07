#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Updated)
"""
import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
from PIL import Image
import datetime

# Optional barcode support
try:
    from pyzbar.pyzbar import decode as zbar_decode
except ImportError:
    zbar_decode = None

# ---------- CONFIGURATION ----------
# This script reads its configuration from .streamlit/secrets.toml
SPREADSHEET_ID = st.secrets.get("google_sheet_id")
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name")

# Page layout
st.set_page_config(page_title="Misiddons Book Database", layout="wide")

# ----- DATA & API FUNCTIONS -----

@st.cache_resource
def connect_to_gsheets() -> gspread.Client:
    """Connect to Google Sheets using service account info from Streamlit secrets."""
    if "gcp_service_account" not in st.secrets:
        st.error("gcp_service_account block not found in Streamlit secrets.")
        return None
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Failed to authorize with provided service account: {e}")
        return None

@st.cache_data(ttl=60)
def load_data(_client: gspread.Client, worksheet_name: str) -> pd.DataFrame:
    """Fetch a worksheet into a DataFrame. The client is passed to control caching."""
    if not _client:
        return pd.DataFrame()
    try:
        ss = _client.open_by_key(SPREADSHEET_ID)
        worksheet = ss.worksheet(worksheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        return df.dropna(how="all")
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Worksheet '{worksheet_name}' not found. Please create it in your Google Sheet.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading worksheet '{worksheet_name}': {e}")
        return pd.DataFrame()

@st.cache_data(ttl=86400)
def get_book_details(isbn: str) -> dict:
    """Fetch book metadata from the Google Books API."""
    if not isbn or len(isbn) < 10:
        return {}
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    try:
        r = requests.get(url)
        r.raise_for_status()  # Raise an exception for bad status codes
        return r.json().get("items", [{}])[0]
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to Google Books API: {e}")
        return {}

@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list:
    """Fetch books by the same author from the Google Books API."""
    if not author:
        return []
    url = f"https://www.googleapis.com/books/v1/volumes?q=inauthor:{quote(author)}"
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.json().get("items", [])
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get recommendations: {e}")
        return []

# ----- MAIN APP -----
st.title("📚 Misiddons Book Database")

client = connect_to_gsheets()

if not client:
    st.warning("Could not connect to Google Sheets. Please check your secret credentials.")
    st.stop()

# ----- Add Book Form -----
with st.form("entry_form"):
    st.subheader("Add a New Book")
    cols = st.columns(5)
    title = cols[0].text_input("Title", key="title")
    author = cols[1].text_input("Author", key="author")
    isbn = cols[2].text_input("ISBN (Optional)", key="isbn")
    # UPDATED: Using st.date_input for better UX
    date_read = cols[3].date_input("Date Read", value=None, key="date_read", help="The date you finished reading the book.")
    choice = cols[4].radio("Add to:", ["Library", "Wishlist"], horizontal=True, key="choice")

    if st.form_submit_button("Add Book"):
        if title and author:
            try:
                ss = client.open_by_key(SPREADSHEET_ID)
                ws = ss.worksheet(choice)
                # Format date for consistent storage in Google Sheets
                date_str = date_read.strftime("%Y-%m-%d") if date_read else ""
                ws.append_row([title, author, isbn, date_str])
                st.success(f"Added '{title}' to your {choice}!")
                # UPDATED: Use st.rerun() to refresh the app state
                st.rerun()
            except Exception as e:
                st.error(f"Failed to add book: {e}")
        else:
            st.warning("Please enter at least a title and an author.")

# ----- Barcode Scanner (Optional) -----
if zbar_decode:
    with st.expander("📷 Scan Barcode"):
        uploaded_image = st.file_uploader("Upload an image of a barcode", type=["png", "jpg", "jpeg"])
        if uploaded_image:
            img = Image.open(uploaded_image)
            decoded_objects = zbar_decode(img)
            if decoded_objects:
                isbn_from_barcode = decoded_objects[0].data.decode("utf-8")
                st.info(f"Found ISBN: {isbn_from_barcode}")
                book_info = get_book_details(isbn_from_barcode).get("volumeInfo", {})
                if book_info:
                    st.text_input("Scanned Title", book_info.get("title", ""), key="scanned_title")
                    st.text_input("Scanned Author(s)", ", ".join(book_info.get("authors", [])), key="scanned_author")
                    st.text_input("Scanned ISBN", isbn_from_barcode, key="scanned_isbn")
                else:
                    st.warning("Could not retrieve book details for this ISBN.")
            else:
                st.warning("No barcode found in the uploaded image.")

st.divider()

# ----- Data Display Tabs -----
library_df = load_data(client, "Library")
wishlist_df = load_data(client, "Wishlist")

tab1, tab2, tab3 = st.tabs(["My Library", "My Wishlist", "Recommendations"])

with tab1:
    st.header("My Library")
    if not library_df.empty:
        st.dataframe(library_df, use_container_width=True)
    else:
        st.info("Your library is empty. Add a book using the form above.")

with tab2:
    st.header("My Wishlist")
    if not wishlist_df.empty:
        st.dataframe(wishlist_df, use_container_width=True)
    else:
        st.info("Your wishlist is empty. Add a book using the form above.")

with tab3:
    st.header("Author Recommendations")
    if not library_df.empty and "Author" in library_df.columns:
        authors = library_df["Author"].dropna().unique()
        selected_author = st.selectbox("See more books by:", authors)
        if selected_author:
            recommendations = get_recommendations_by_author(selected_author)
            if recommendations:
                for item in recommendations:
                    vol_info = item.get("volumeInfo", {})
                    rec_title = vol_info.get("title", "No Title Available")
                    rec_authors = ", ".join(vol_info.get("authors", ["N/A"]))
                    
                    # Avoid showing books already in the library
                    if rec_title not in library_df["Title"].values:
                        st.subheader(rec_title)
                        st.write(f"**By:** {rec_authors}")
                        thumbnail = vol_info.get("imageLinks", {}).get("thumbnail")
                        if thumbnail:
                            st.image(thumbnail)
                        st.markdown("---")
            else:
                st.info(f"No other books found for {selected_author}.")
    else:
        st.info("Add books to your library to get author recommendations.")
