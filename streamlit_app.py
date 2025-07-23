#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit Book Database App with Ratings
"""

import streamlit as st
import pandas as pd
from pyzbar.pyzbar import decode
from PIL import Image
import requests
import random

# Configure page (must be first Streamlit command)
st.set_page_config(page_title="misiddons Book Database App", layout="wide")

# Global CSS to enforce side-by-side columns without wrapping
st.markdown(
    """
    <style>
    /* Prevent columns from wrapping on small screens */
    [data-testid=column]:not(:last-child) {
        margin-right: 1rem;
    }
    /* Ensure buttons fill their column */
    .stButton > button {
        width: 100%;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Filepaths for library and wishlist databases
BOOK_DB = "books_database.csv"
WISHLIST_DB = "wishlist_database.csv"

# --- Database utilities ---
def load_db(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype={"ISBN": str})
    except FileNotFoundError:
        df = pd.DataFrame(columns=["ISBN", "Title", "Author", "Genre", "Language", "Thumbnail", "Description", "Rating"])
    if "Rating" not in df.columns:
        df["Rating"] = pd.NA
    else:
        df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    return df


def save_db(df: pd.DataFrame, path: str) -> pd.DataFrame:
    df = df.copy()
    df["ISBN"] = df["ISBN"].astype(str)
    if "Rating" not in df.columns:
        df["Rating"] = pd.NA
    df.to_csv(path, index=False)
    return df

# --- Barcode scanning ---
def scan_barcode(image: Image.Image) -> str | None:
    decoded = decode(image)
    return decoded[0].data.decode("utf-8") if decoded else None

# --- Fetch book details from Google Books API ---
def fetch_book_details(isbn: str) -> dict | None:
    try:
        resp = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}", timeout=5)
        resp.raise_for_status()
        items = resp.json().get("items")
        if items:
            info = items[0]["volumeInfo"]
            title = info.get("title", "Unknown Title")
            authors = ", ".join(info.get("authors", ["Unknown Author"]))
            genres = ", ".join(info.get("categories", ["Unknown Genre"]))
            language = info.get("language", "").upper() or "Unknown"
            thumb = info.get("imageLinks", {}).get("thumbnail", "")
            desc = info.get("description", "No description available.")
            if len(desc) > 300:
                desc = desc[:300] + "..."
            return {"Title": title, "Author": authors, "Genre": genres,
                    "Language": language, "Thumbnail": thumb,
                    "Description": desc, "Rating": pd.NA}
    except requests.RequestException:
        return None
    return None

# --- Recommendations by author (cached) ---
@st.cache_data
def get_recommendations_by_author(author: str, max_results: int = 5) -> list[dict]:
    try:
        resp = requests.get(
            f"https://www.googleapis.com/books/v1/volumes?q=inauthor:{author}&maxResults={max_results}",
            timeout=5
        )
        resp.raise_for_status()
        recs = []
        for item in resp.json().get("items", []):
            info = item.get("volumeInfo", {})
            thumb = info.get("imageLinks", {}).get("thumbnail", "")
            desc = info.get("description", "No description available.")
            recs.append({
                "Title": info.get("title", "Unknown Title"),
                "Authors": ", ".join(info.get("authors", ["Unknown Author"])),
                "Year": info.get("publishedDate", "").split("-")[0] or "Unknown",
                "Rating": info.get("averageRating", "N/A"),
                "Thumbnail": thumb,
                "Description": desc
            })
        return recs
    except requests.RequestException:
        return []

# --- Initialize session state ---
if "library" not in st.session_state:
    st.session_state["library"] = load_db(BOOK_DB)
if "wishlist" not in st.session_state:
    st.session_state["wishlist"] = load_db(WISHLIST_DB)

library_df = st.session_state["library"].copy()
wishlist_df = st.session_state["wishlist"].copy()

if "Rating" not in library_df.columns:
    library_df["Rating"] = pd.NA

# --- App UI ---
st.title("Misiddons")

# --- Unified Scan Section ---
st.subheader("Scan Barcode to Add to Library or Wishlist")
book_file = st.file_uploader(
    "Upload a barcode image to add a book", type=["jpg", "jpeg", "png"], key="scan_book"
)
if book_file:
    img = Image.open(book_file)
    isbn = scan_barcode(img)
    if isbn:
        st.success(f"ISBN Scanned: {isbn}")
        in_lib = isbn in library_df["ISBN"].values
        in_wish = isbn in wishlist_df["ISBN"].values
        if in_lib or in_wish:
            if in_lib and in_wish:
                st.warning("Book already in library and wishlist.")
            elif in_lib:
                st.warning("Book already in library.")
            else:
                st.warning("Book already on wishlist.")
        else:
            with st.spinner("Fetching book details..."):
                details = fetch_book_details(isbn)
            if details:
                col1, col2 = st.columns([1, 3])
                thumb_url = details.get("Thumbnail", "")
                if thumb_url.startswith("http"):
                    try:
                        st.image(thumb_url, width=120)
                    except Exception:
                        pass
                with col2:
                    st.markdown(f"### {details['Title']}")
                    st.write(f"*{details['Author']}*")
                    st.write(details['Description'])

                btn_col1, btn_col2 = st.columns(2, gap="small")
                with btn_col1:
                    if st.button("Add to Library", key=f"add_lib_{isbn}"):
                        library_df = pd.concat([
                            library_df, pd.DataFrame([{"ISBN": isbn, **details}])
                        ], ignore_index=True)
                        st.session_state["library"] = save_db(library_df, BOOK_DB)
                        st.success("Added to Library!")
                with btn_col2:
                    if st.button("Add to Wishlist", key=f"add_wish_{isbn}"):
                        wishlist_df = pd.concat([
                            wishlist_df, pd.DataFrame([{"ISBN": isbn, **details}])
                        ], ignore_index=True)
                        st.session_state["wishlist"] = save_db(wishlist_df, WISHLIST_DB)
                        st.success("Added to Wishlist!")
            else:
                st.error("Could not fetch book details.")
    else:
        st.error("No barcode detected.")

# --- Search for a Book ---
st.subheader("Search for a Book")
search_q = st.text_input("Enter title, author, or ISBN to search your library")
if search_q:
    res = library_df[
        library_df["Title"].str.contains(search_q, case=False, na=False) |
        library_df["Author"].str.contains(search_q, case=False, na=False) |
        library_df["ISBN"].str.contains(search_q, case=False, na=False)
    ]
    if not res.empty:
        st.dataframe(res)
    else:
        st.write("No matches found.")

# --- Rate Your Books ---
st.subheader("Rate Your Books")

# Identify all non-rated books
onerated = library_df[library_df["Rating"].isna()]

if not onerated.empty:
    if (
        "rate_current_isbn" not in st.session_state
        or st.session_state["rate_current_isbn"] not in onerated["ISBN"].tolist()
    ):
        book = onerated.sample(1, random_state=random.randint(0, 10000)).iloc[0]
        st.session_state["rate_current_isbn"] = book["ISBN"]
    else:
        book = library_df.loc[
            library_df["ISBN"] == st.session_state["rate_current_isbn"]
        ].iloc[0]

    idx0 = library_df.index[library_df["ISBN"] == book["ISBN"]][0]

    st.markdown(f"**Title:** {book['Title']}")
    st.markdown(f"**Author:** {book['Author']}")

    thumb = book.get("Thumbnail", "")
    if isinstance(thumb, str) and thumb.startswith("http"):
        st.image(thumb, width=150)
    else:
        st.write("*(No cover available)*")

    rating_key = f"rate_{book['ISBN']}"
    if rating_key not in st.session_state:
        st.session_state[rating_key] = int(library_df.at[idx0, "Rating"]) if pd.notna(library_df.at[idx0, "Rating"]) else 3

    rating = st.radio(
        label="",
        options=[1, 2, 3, 4, 5],
        format_func=lambda x: "★" * x + "☆" * (5 - x),
        key=rating_key,
        horizontal=True
    )

    if st.button("Save Rating", key=f"save_rate_{book['ISBN']}"):
        library_df.at[idx0, "Rating"] = rating
        st.session_state["library"] = save_db(library_df, BOOK_DB)
        st.success(f"Saved rating {rating} for '{book['Title']}'")
        del st.session_state["rate_current_isbn"]
        del st.session_state[rating_key]
else:
    st.info("All books are rated!")

# --- Display Library ---
st.subheader("My Library")
if not library_df.empty:
    st.dataframe(library_df.iloc[::-1].reset_index(drop=True))
else:
    st.info("Library is empty.")

# --- Wishlist Section ---
st.subheader("My Wishlist")
if not wishlist_df.empty:
    st.dataframe(wishlist_df.iloc[::-1].reset_index(drop=True))
else:
    st.info("Wishlist is empty.")

# --- Summary Statistics ---
st.subheader("Library Summary")
total = len(library_df)
st.metric("Total Books", total)
if not library_df.empty:
    st.write("#### Language Distribution")
    for lang, cnt in library_df['Language'].value_counts().items():
        st.write(f"- {lang}: {cnt}")
    st.write("#### Top 5 Authors")
    auth = library_df['Author'].str.split(', ').explode().value_counts().head(5)
    st.bar_chart(auth)
else:
    st.info("No data available.")

# --- Top Rated Books ---
st.subheader("Top Rated Books")
top_rated = library_df[library_df["Rating"].notna()] \
                   .sort_values("Rating", ascending=False).head(5)

if not top_rated.empty:
    for _, book in top_rated.iterrows():
        c1, c2, c3 = st.columns([1, 3, 1], gap="small")
        thumb = book.get("Thumbnail", "")
        if isinstance(thumb, str) and thumb.startswith("http"):
            c1.image(thumb, width=100)
        else:
            c1.write("*(No cover)*")
        with c2:
            st.markdown(f"**{book['Title']}**")
            st.write(f"_by {book['Author']}_")
        stars = "★" * int(book["Rating"]) + "☆" * (5 - int(book["Rating"]))
        c3.markdown(f"**{stars}**")
        st.markdown("---")
else:
    st.info("You haven’t rated any books yet!")

# --- Recommended Books from Your Favorite Authors ---
st.subheader("Recommended Books from Your Favorite Authors")
fav = None
if not library_df.empty:
    fav = st.selectbox("Select an author:", library_df['Author'].unique().tolist())
if fav:
    recs = get_recommendations_by_author(fav)
    for rec in recs:
        with st.container():
            col1, col2 = st.columns([1,3])
            thumb_url = rec.get("Thumbnail", "")
            if isinstance(thumb_url, str) and thumb_url.startswith("http"):
                try:
                    col1.image(thumb_url, width=120)
                except Exception:
                    pass
            with col2:
                st.markdown(f"**{rec['Title']}** by {rec['Authors']}")
                st.write(f"Year: {rec['Year']}, Rating: {rec['Rating']}")
                st.write(rec['Description'])
            st.markdown("---")
