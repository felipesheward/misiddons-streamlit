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

# ---------- CONFIGURATION ----------
# IMPORTANT: Set your Google Sheet name here
GOOGLE_SHEET_NAME = "Misiddons Book Databse" # Or whatever your sheet is named

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
    """Establish and cache a connection to Google Sheets."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scopes
        )
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets. Please check your credentials. Error: {e}")
        return None

@st.cache_data(ttl=60)
def load_data(client: gspread.Client, worksheet_name: str) -> pd.DataFrame:
    """Safely fetch a worksheet and return it as a pandas DataFrame."""
    if client is None:
        return pd.DataFrame()
    try:
        spreadsheet = client.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(worksheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        return df.dropna(axis=0, how="all")
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Spreadsheet '{GOOGLE_SHEET_NAME}' not found. Please check the name in the script and your sharing permissions.")
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Worksheet '{worksheet_name}' not found. Please create a tab with this exact name.")
    except Exception as e:
        st.error(f"An error occurred while loading data from worksheet '{worksheet_name}': {e}")
    return pd.DataFrame()

@st.cache_data(ttl=86400)
def get_book_details(isbn: str) -> dict:
    if not isbn or not isinstance(isbn, str) or len(isbn) < 10: return {}
    r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}")
    if r.status_code == 200:
        data = r.json()
        if "items" in data: return data["items"][0]
    return {}

@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list:
    if not author: return []
    safe_author = quote(author)
    r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=inauthor:{safe_author}")
    if r.status_code == 200:
        data = r.json()
        if "items" in data: return data["items"]
    return []


# ---------- Main App Logic ----------

client = connect_to_gsheets()

library_df = load_data(client, "Library")
wishlist_df = load_data(client, "Wishlist")

if client and (library_df.empty and wishlist_df.empty):
    st.warning("Could not load data from 'Library' or 'Wishlist'. Please check the error messages above and refresh the page.")
    st.stop()

# ----- APP UI -----

st.title("Misiddons Book Database")

st.header("Add a new book")
with st.form(key="entry_form"):
    form_cols = st.columns([1, 1, 1, 1, 1])
    title = form_cols[0].text_input("Title")
    author = form_cols[1].text_input("Author")
    isbn = form_cols[2].text_input("ISBN")
    date_read = form_cols[3].text_input("Date Read")
    list_choice = form_cols[4].radio("Add to list:", ["Library", "Wishlist"], horizontal=True)
    
    if st.form_submit_button("Add Book"):
        if title and author:
            try:
                sheet_to_update = client.open(GOOGLE_SHEET_NAME).worksheet(list_choice)
                # Append a row with the correct column order
                sheet_to_update.append_row([title, author, isbn, date_read])
                st.success(f"'{title}' added to your {list_choice}!")
                st.cache_data.clear() # Refresh data
            except Exception as e:
                st.error(f"Failed to add book: {e}")
        else:
            st.warning("Please enter at least a title and author.")


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
