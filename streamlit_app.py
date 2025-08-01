#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app
"""

from __future__ import annotations

import random
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st
from streamlit_gsheets import GSheetsConnection
from PIL import Image, ImageOps
conn = st.connection("gsheets", type=GSheetsConnection)

# ---------- OPTIONAL barcode support ----------
try:
    from pyzbar.pyzbar import decode as zbar_decode
except Exception:   # pyzbar / libzbar missing in environment
    zbar_decode = None

# ---------- Streamlit config ----------
st.set_page_config(page_title="Misiddons Book Database", layout="wide")
st.markdown(
    """
    <style>
    [data-testid=column]:not(:last-child){margin-right:1rem;}
    .stButton > button{width:100%; text-wrap:balance;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- Paths ----------
BASE = Path(__file__).parent
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(exist_ok=True)

BOOK_DB = DATA_DIR / "books_database.csv"
WISHLIST_DB = DATA_DIR / "wishlist_database.csv"

# ---------- Data helpers ----------


@st.cache_data(ttl=0)      # always get a fresh copy
def load_sheet(tab: str) -> pd.DataFrame:
    """
    Fetch the entire Google-Sheets *tab* and return it as a DataFrame.
    `tab` must match the worksheet name exactly (e.g., 'Library').
    """
    return conn.read(worksheet=tab)



# ---------- NEW – persistence wrapper ----------

def sync_session(name: str) -> None:
    """
    Push the DataFrame stored in st.session_state[name] to the worksheet
    whose title is `name.capitalize()` ('library' → 'Library').
    """
    conn.update(
        worksheet=name.capitalize(),
        data=st.session_state[name]      # the DataFrame to write
    )


# ---------- Barcode utilities ----------

def scan_barcode(image: Image.Image) -> str | None:
    """Try to decode a barcode (EAN/ISBN) from *image*."""
    if zbar_decode is None:
        return None
    img = ImageOps.exif_transpose(image).convert("RGB")
    res = zbar_decode(img)
    if not res:  # try up‑scaled version for low‑res photos
        big = img.resize((img.width * 2, img.height * 2))
        res = zbar_decode(big)
    return res[0].data.decode("utf-8") if res else None


# ---------- Book API helpers ----------

def _clip(text: str | None, n: int = 300) -> str:
    text = (text or "").strip()
    return text[:n] + ("..." if len(text) > n else "") if text else "No description available."


def _norm_lang(code: str | None) -> str:
    return (code or "").upper() or "Unknown"


# -- Google Books --

def fetch_from_google(isbn: str) -> dict | None:
    url = "https://www.googleapis.com/books/v1/volumes"
    r = requests.get(
        url,
        params={"q": f"isbn:{isbn}"},
        timeout=12,
        headers={"User-Agent": "misiddons/1.0"},
    )
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        return None
    info = items[0].get("volumeInfo", {})
    desc = info.get("description") or items[0].get("searchInfo", {}).get("textSnippet")
    return {
        "Title": info.get("title", "Unknown Title"),
        "Author": ", ".join(info.get("authors", ["Unknown Author"])),
        "Genre": ", ".join(info.get("categories", ["Unknown Genre"])),
        "Language": _norm_lang(info.get("language")),
        "Thumbnail": info.get("imageLinks", {}).get("thumbnail", ""),
        "Description": _clip(desc),
        "Rating": pd.NA,
    }


# -- OpenLibrary fallback --

def fetch_from_openlibrary(isbn: str) -> dict | None:
    r = requests.get(f"https://openlibrary.org/isbn/{isbn}.json", timeout=12)
    if r.status_code != 200:
        return None
    j = r.json()
    title = j.get("title", "Unknown Title")

    # Authors
    authors = []
    for a in j.get("authors", []):
        ar = requests.get(f"https://openlibrary.org{a['key']}.json", timeout=6)
        if ar.ok:
            authors.append(ar.json().get("name", "Unknown Author"))

    cover_id = j.get("covers", [None])[0]
    thumb = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else ""

    desc = j.get("description", "")
    if isinstance(desc, dict):
        desc = desc.get("value", "")

    lang = "Unknown"
    if j.get("languages"):
        lang = j["languages"][0].get("key", "").split("/")[-1].upper() or "Unknown"

    return {
        "Title": title,
        "Author": ", ".join(authors) if authors else "Unknown Author",
        "Genre": "Unknown",
        "Language": lang,
        "Thumbnail": thumb,
        "Description": _clip(desc),
        "Rating": pd.NA,
    }


# -- unified fetch --

def fetch_book_details(isbn: str) -> dict | None:
    isbn = isbn.replace("-", "").strip()
    try:
        if details := fetch_from_google(isbn):
            return details
    except Exception:
        pass
    try:
        return fetch_from_openlibrary(isbn)
    except Exception:
        return None


# ---------- Recommendations (cached) ----------

@st.cache_data(ttl=3600, show_spinner=False)
def get_recommendations_by_author(author: str, max_results: int = 5) -> list[dict]:
    """Pull up to *max_results* books by *author* from Google Books/OpenLibrary."""

    def clip(t: str | None, n: int = 300):
        t = (t or "").strip()
        return t[:n] + ("..." if len(t) > n else "") if t else "No description available."

    out: list[dict] = []

    # ---- Google Books ----
    try:
        url = "https://www.googleapis.com/books/v1/volumes"
        params = {"q": f'inauthor:"{author}"', "maxResults": max_results}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        for item in r.json().get("items", []):
            info = item.get("volumeInfo", {})
            desc = info.get("description") or item.get("searchInfo", {}).get("textSnippet")
            out.append({
                "Title": info.get("title", "Unknown Title"),
                "Authors": ", ".join(info.get("authors", ["Unknown Author"])),
                "Year": info.get("publishedDate", "").split("-")[0] or "Unknown",
                "Rating": info.get("averageRating", "N/A"),
                "Thumbnail": info.get("imageLinks", {}).get("thumbnail", ""),
                "Description": clip(desc)
            })
    except Exception:
        pass

    if len(out) >= max_results:
        return out[:max_results]

    # ---- OpenLibrary fallback ----
    try:
        url = f"https://openlibrary.org/search.json?author={quote(author)}&limit={max_results}"
        r = requests.get(url, timeout=8)
        if r.ok:
            for d in r.json().get("docs", []):
                cover_id = d.get("cover_i")
                thumb = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else ""
                desc = d.get("first_sentence") or d.get("subtitle") or ""
                out.append({
                    "Title": d.get("title", "Unknown Title"),
                    "Authors": ", ".join(d.get("author_name", ["Unknown Author"])),
                    "Year": str(d.get("first_publish_year", "Unknown")),
                    "Rating": "N/A",
                    "Thumbnail": thumb,
                    "Description": clip(desc)
                })
    except Exception:
        pass

    return out[:max_results]

# ---------- Session state ----------
if "library" not in st.session_state:
    st.session_state["library"] = load_sheet("Library")
if "wishlist" not in st.session_state:
    st.session_state["wishlist"] = load_sheet("Wishlist")

library_df = st.session_state["library"]      
wishlist_df = st.session_state["wishlist"]       

# ---------- UI ----------
st.title("Misiddons Book Database")

# --- Scan section ---
st.subheader("Scan Barcode to Add to Library or Wishlist")
book_file = st.file_uploader("Upload a barcode image to add a book",
                             type=["jpg","jpeg","png"], key="scan_book")

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

            if not details:
                st.error("Could not fetch book details.")
                manual_title = st.text_input("Enter title manually:")
                if manual_title:
                    details = {
                        "Title": manual_title,
                        "Author": "Unknown",
                        "Genre": "Unknown",
                        "Language": "Unknown",
                        "Thumbnail": "",
                        "Description": "",
                        "Rating": pd.NA
                    }

            if details:
                col1, col2 = st.columns([1,3])
                if details.get("Thumbnail","").startswith("http"):
                    col1.image(details["Thumbnail"], width=120)
                with col2:
                    st.markdown(f"### {details['Title']}")
                    st.write(f"*{details['Author']}*")
                    st.write(details['Description'])

                b1, b2 = st.columns(2)
                with b1:
                    if st.button("Add to Library", key=f"add_lib_{isbn}"):
                        library_df.loc[len(library_df)] = {"ISBN": isbn, **details}
                        sync_session("library")
                        st.success("Added to Library!")
                with b2:
                    if st.button("Add to Wishlist", key=f"add_wish_{isbn}"):
                        wishlist_df.loc[len(wishlist_df)] = {"ISBN": isbn, **details}
                        sync_session("wishlist")
                        st.success("Added to Wishlist!")
    else:
        if zbar_decode is None:
            st.error("Barcode scanning module unavailable. Type the ISBN below.")
        else:
            st.error("No barcode detected. Try a clearer photo or enter ISBN manually.")
        manual_isbn = st.text_input("ISBN (manual):", "")
        if manual_isbn:
            with st.spinner("Fetching book details..."):
                details = fetch_book_details(manual_isbn)
            if details:
                st.success(f"Got details for {manual_isbn}")
                col1, col2 = st.columns([1,3])
                if details.get("Thumbnail","").startswith("http"):
                    col1.image(details["Thumbnail"], width=120)
                with col2:
                    st.markdown(f"### {details['Title']}")
                    st.write(f"*{details['Author']}*")
                    st.write(details['Description'])
                b1, b2 = st.columns(2)
                with b1:
                    if st.button("Add to Library", key=f"add_lib_manual_{manual_isbn}"):
                        library_df.loc[len(library_df)] = {"ISBN": manual_isbn, **details}
                        sync_session("library")
                        st.success("Added to Library!")
                with b2:
                    if st.button("Add to Wishlist", key=f"add_wish_manual_{manual_isbn}"):
                        wishlist_df.loc[len(wishlist_df)] = {"ISBN": manual_isbn, **details}
                        sync_session("wishlist")
                        st.success("Added to Wishlist!")

# --- Search ---
st.subheader("Search for a Book")
search_q = st.text_input("Enter title, author, or ISBN to search your library")
if search_q:
    res = library_df[
        library_df["Title"].str.contains(search_q, case=False, na=False) |
        library_df["Author"].str.contains(search_q, case=False, na=False) |
        library_df["ISBN"].str.contains(search_q, case=False, na=False)
    ]
    st.dataframe(res if not res.empty else pd.DataFrame())

# --- Rate books ---
st.subheader("Rate Your Books")
unrated = library_df[library_df["Rating"].isna()]
if not unrated.empty:
    if ("rate_current_isbn" not in st.session_state
        or st.session_state["rate_current_isbn"] not in unrated["ISBN"].tolist()):
        book = unrated.sample(1, random_state=random.randint(0, 10000)).iloc[0]
        st.session_state["rate_current_isbn"] = book["ISBN"]
    else:
        book = library_df.loc[
            library_df["ISBN"] == st.session_state["rate_current_isbn"]
        ].iloc[0]

    idx0 = library_df.index[library_df["ISBN"] == book["ISBN"]][0]
    st.markdown(f"**Title:** {book['Title']}  \n**Author:** {book['Author']}")
    if isinstance(book.get("Thumbnail",""), str) and book["Thumbnail"].startswith("http"):
        st.image(book["Thumbnail"], width=150)

    rating_key = f"rate_{book['ISBN']}"
    if rating_key not in st.session_state:
        st.session_state[rating_key] = (
            int(library_df.at[idx0,"Rating"])
            if pd.notna(library_df.at[idx0,"Rating"]) else 3
        )

    rating = st.radio(
        label="",
        options=[1,2,3,4,5],
        format_func=lambda x: "★"*x + "☆"*(5-x),
        key=rating_key,
        horizontal=True
    )

    if st.button("Save Rating", key=f"save_rate_{book['ISBN']}"):
        library_df.at[idx0, "Rating"] = rating
        sync_session("library")
        st.success(f"Saved rating {rating} for '{book['Title']}'")
        del st.session_state["rate_current_isbn"]
        del st.session_state[rating_key]
else:
    st.info("All books are rated!")

# --- Library table ---
st.subheader("My Library")
if not library_df.empty:
    st.dataframe(library_df.iloc[::-1].reset_index(drop=True))
else:
    st.info("Library is empty.")

# --- Wishlist ---
st.subheader("My Wishlist")
if not wishlist_df.empty:
    st.dataframe(wishlist_df.iloc[::-1].reset_index(drop=True))
else:
    st.info("Wishlist is empty.")

# --- Summary ---
st.subheader("Library Summary")
total = len(library_df)
st.metric("Total Books", total)
if total:
    st.write("#### Language Distribution")
    for lang, cnt in library_df["Language"].value_counts().items():
        st.write(f"- {lang}: {cnt}")


# --- Top Rated (Top 10: only Title, Author, Rating) ---
st.subheader("Top Rated Books (Top 10)")

cols = ["Title", "Author", "Rating"]
top_rated = (
    library_df.dropna(subset=["Rating"])
              .sort_values(["Rating", "Title"], ascending=[False, True])
              .head(10)[cols]
)

if top_rated.empty:
    st.info("You haven’t rated any books yet!")
else:
    for i, row in enumerate(top_rated.itertuples(index=False), 1):
        st.markdown(f"**{i}. {row.Title}** — {row.Author} | {row.Rating}/5")

# --- Top 5 Authors WITH titles ---
st.subheader("Top 5 Authors (with your titles)")
if not library_df.empty:
    auth_df = (
        library_df.assign(Author=library_df["Author"].str.split(","))
                  .explode("Author")
                  .assign(Author=lambda d: d["Author"].str.strip())
    )

    top_authors = (
        auth_df.groupby("Author")
               .agg(
                   BookCount=("Title", "size"),
                   Titles=("Title", lambda s: sorted(set(s)))
               )
               .sort_values("BookCount", ascending=False)
               .head(5)
    )

    for i, (author, row) in enumerate(top_authors.iterrows(), start=1):
        st.markdown(f"**{i}. {author}** — {row.BookCount} book(s)")
        for title in row.Titles:
            st.write(f"- {title}")
        st.markdown("---")
else:
    st.info("Library is empty.")

# --- Recommendations ---
st.subheader("Recommended Books from Your Favorite Authors")

if not library_df.empty:
    author_list = (library_df["Author"]
                   .str.split(",")
                   .explode()
                   .str.strip()
                   .dropna()
                   .unique()
                   .tolist())

    fav_author = st.selectbox("Select an author:", sorted(author_list), key="rec_author")

    # manual refresh button to bust cache if needed
    if st.button("Get recommendations", key="get_recs_btn"):
        st.cache_data.clear()

    if fav_author:
        with st.spinner(f"Fetching books by {fav_author}..."):
            recs = get_recommendations_by_author(fav_author)

        st.write(f"**Found {len(recs)} recommendations.**")

        if not recs:
            st.warning("No recommendations came back. Try another author or tap 'Get recommendations' again.")
        else:
            for rec in recs:
                with st.container():
                    col1, col2 = st.columns([1,3])
                    if rec.get("Thumbnail","").startswith("http"):
                        col1.image(rec["Thumbnail"], width=120)
                    with col2:
                        st.markdown(f"**{rec['Title']}** by {rec['Authors']}")
                        st.write(f"Year: {rec['Year']}  |  Rating: {rec['Rating']}")
                        st.write(rec["Description"] or "No description available.")
                st.markdown("---")
