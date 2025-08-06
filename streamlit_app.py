#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app
"""

import pandas as pd
import requests
import streamlit as st
from gspread.exceptions import WorksheetNotFound
from streamlit_gsheets import GSheetsConnection
from urllib.parse import quote
from PIL import Image, ImageOps
import io

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

@st.cache_resource(ttl=3600)
def get_connection():
    """Create and cache the connection to Google Sheets."""
    return st.connection("gsheets", type=GSheetsConnection)

@st.cache_data(ttl=60)
def load_data(worksheet_name: str) -> pd.DataFrame:
    """Safely fetch a worksheet from Google Sheets."""
    try:
        conn = get_connection()
        sheet = conn.read(worksheet=worksheet_name, usecols=list(range(20)), ttl=60)
        return sheet.dropna(axis=0, how="all")
    except WorksheetNotFound:
        # Return an empty DataFrame with expected columns to prevent other errors
        st.error(f"Worksheet '{worksheet_name}' not found in your Google Sheet. Please create a tab with this exact name.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An error occurred while loading data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=86400) # Cache for one day
def get_book_details(isbn: str) -> dict:
    """Fetch book details from Google Books API by ISBN."""
    if not isbn or not isinstance(isbn, str) or len(isbn) < 10:
        return {}
    r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}")
    if r.status_code == 200:
        data = r.json()
        if "items" in data:
            return data["items"][0]
    return {}

@st.cache_data(ttl=86400) # Cache for one day
def get_recommendations_by_author(author: str) -> list:
    """Fetch recommendations from Google Books API by author."""
    if not author:
        return []
    safe_author = quote(author)
    r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=inauthor:{safe_author}")
    if r.status_code == 200:
        data = r.json()
        if "items" in data:
            return data["items"]
    return []

# ---------- Main App Logic ----------

# Establish connection
conn = get_connection()

# Load data with error handling
library_df = load_data(worksheet_name="Library")
wishlist_df = load_data(worksheet_name="Wishlist")

# Stop the app if essential sheets failed to load
if library_df.empty and wishlist_df.empty:
    st.error("Could not load 'Library' or 'Wishlist' worksheets. Please check your Google Sheet and refresh.")
    st.stop()

# ----- APP UI -----

st.title("Misiddons Book Database")

# ---------- Book Entry Form ----------
st.header("Add a new book")
entry_form = st.form(key="entry_form")
form_cols = entry_form.columns([1, 1, 1, 1, 1])

with form_cols[0]:
    title = st.text_input("Title", key="title_in")
with form_cols[1]:
    author = st.text_input("Author", key="author_in")
with form_cols[2]:
    isbn = st.text_input("ISBN", key="isbn_in")
with form_cols[3]:
    date_read = st.text_input("Date Read", key="date_in")
with form_cols[4]:
    list_choice = st.radio("Add to list:", ["Library", "Wishlist"], horizontal=True)

submit_button = entry_form.form_submit_button(label="Add Book")

if submit_button:
    if title and author:
        new_book_data = pd.DataFrame([{
            "Title": title,
            "Author": author,
            "ISBN": isbn,
            "Date Read": date_read
        }])
        target_df = library_df if list_choice == "Library" else wishlist_df
        updated_df = pd.concat([target_df, new_book_data], ignore_index=True)
        conn.update(worksheet=list_choice, data=updated_df)
        st.success(f"'{title}' added to your {list_choice}!")
        st.cache_data.clear() # Clear cache to show new entry
    else:
        st.warning("Please enter a title and author.")

# ---------- Barcode Scanner (Optional) ----------
if zbar_decode:
    st.header("Scan Barcode")
    uploaded_image = st.file_uploader("Upload an image with a barcode", type=["png", "jpg", "jpeg"])
    if uploaded_image:
        image = Image.open(uploaded_image)
        barcodes = zbar_decode(image)
        if barcodes:
            for barcode in barcodes:
                isbn_from_barcode = barcode.data.decode("utf-8")
                st.info(f"Detected ISBN: {isbn_from_barcode}")
                book_info = get_book_details(isbn_from_barcode)
                if book_info:
                    volume_info = book_info.get("volumeInfo", {})
                    st.text_input("Title", value=volume_info.get("title", ""), key="barcode_title")
                    st.text_input("Author", value=", ".join(volume_info.get("authors", [])), key="barcode_author")
                    st.text_input("ISBN", value=isbn_from_barcode, key="barcode_isbn")
                else:
                    st.error("Could not find book details for this ISBN.")
        else:
            st.warning("No barcode found in the uploaded image.")

st.divider()

# ---------- Display Data ----------
tab1, tab2, tab3 = st.tabs(["Library", "Wishlist", "Recommendations"])

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
        st.info("Your wishlist is empty.")

with tab3:
    st.header("Recommendations")
    if not library_df.empty and "Author" in library_df.columns:
        authors = library_df["Author"].dropna().unique()
        selected_author = st.selectbox("Find books by authors you've read:", authors)
        if selected_author:
            recommendations = get_recommendations_by_author(selected_author)
            if recommendations:
                for item in recommendations:
                    with st.container():
                        vol_info = item.get("volumeInfo", {})
                        cols = st.columns([1, 4])
                        with cols[0]:
                            if "imageLinks" in vol_info and "thumbnail" in vol_info["imageLinks"]:
                                st.image(vol_info["imageLinks"]["thumbnail"])
                        with cols[1]:
                            st.subheader(vol_info.get("title", "No Title"))
                            st.write(f"**Author(s):** {', '.join(vol_info.get('authors', ['N/A']))}")
                            st.write(f"**Published:** {vol_info.get('publishedDate', 'N/A')}")
                            st.caption(vol_info.get('description', 'No description available.'))
                        st.markdown("---")
            else:
                st.write(f"No recommendations found for {selected_author}.")
    else:
        st.info("Read some books to get recommendations!")
