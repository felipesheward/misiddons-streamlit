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
from PIL import Image, ImageOps

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

def load_db(path: Path) -> pd.DataFrame:
    """Load a CSV into a DataFrame (creating an empty one if absent)."""
    try:
        df = pd.read_csv(path, dtype={"ISBN": str})
    except FileNotFoundError:
        df = pd.DataFrame(
            columns=[
                "ISBN",
                "Title",
                "Author",
                "Genre",
                "Language",
                "Thumbnail",
                "Description",
                "Rating",
            ]
        )

    # Ensure Rating exists & is numeric where possible
    if "Rating" not in df.columns:
        df["Rating"] = pd.NA
    else:
        df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")

    return df


def save_db(df: pd.DataFrame, path: Path) -> None:
    """Persist *df* to *path* as CSV."""
    df = df.copy()
    df["ISBN"] = df["ISBN"].astype(str)
    if "Rating" not in df.columns:
        df["Rating"] = pd.NA
    df.to_csv(path, index=False)


# ---------- NEW – persistence wrapper ----------

def sync_session(name: str) -> None:
    """Write the in‑memory DataFrame to disk & keep session in sync.

    ``name`` must be either "library" or "wishlist".
    """
    if name == "library":
        save_db(st.session_state[name], BOOK_DB)
    elif name == "wishlist":
        save_db(st.session_state[name], WISHLIST_DB)
    else:
        raise ValueError("Unknown DataFrame: " + name)


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
    st.session_state["library"] = load_db(BOOK_DB)
if "wishlist" not in st.session_state:
    st.session_state["wishlist"] = load_db(WISHLIST_DB)

library_df = st.session_state["library"]
wishlist_df = st.session_state["wishlist"]

# ---------- UI ----------
st.title("Misiddons Book Database")

# --- Scan section ---
tab1, tab2 = st.tabs(['📷 Scan barcode', '✍️ Enter ISBN'])
with tab1:
    f = st.file_uploader('Upload barcode image', type=['jpg','jpeg','png'], key='scan')
    isbn_scanned = None
    if f:
        img = Image.open(f)
        isbn_scanned = scan_barcode(img)
        if isbn_scanned:
            st.success(f'ISBN detected: {isbn_scanned}')
            st.image(img, caption=isbn_scanned, width=160)
        else:
            st.error('No barcode detected.')
with tab2:
    isbn_scanned = isbn_scanned or None
    manual = st.text_input('ISBN', key='manual')
    isbn_input = (manual or isbn_scanned or '').strip()

if isbn_input:
    if isbn_input in library_df['ISBN'].values:
        st.warning('Book already in library')
    elif isbn_input in wishlist_df['ISBN'].values:
        st.warning('Book already on wishlist')
    else:
        with st.spinner('Fetching details…'):
            meta = fetch_book_details(isbn_input) or {}
        if not meta:
            st.error('Not found, fill manually')
            meta['Title'] = st.text_input('Title*')
            meta['Author'] = st.text_input('Author', value='Unknown')
            meta['Genre'] = st.text_input('Genre', value='Unknown')
            meta['Language'] = st.text_input('Language', value='Unknown')
            meta['Thumbnail'] = ''
            meta['Description'] = st.text_area('Description')
            meta['Rating'] = pd.NA
        if meta.get('Title'):
            c1, c2 = st.columns([1, 3])
            if meta.get('Thumbnail', '').startswith('http'):
                c1.image(meta['Thumbnail'], width=120)
            with c2:
                st.markdown(f"### {meta['Title']}")
                st.caption(meta['Author'])
                st.write(meta['Description'])
            b1, b2 = st.columns(2)
            with b1:
                if st.button("➕ Add to Library", key=f"add_lib_{isbn_input}"):
                    st.session_state['library'] = pd.concat(
                        [st.session_state['library'], pd.DataFrame([{'ISBN': isbn_input, **meta}])],
                        ignore_index=True
                    )
                    sync_session('library')
                    st.experimental_rerun()
            with b2:
                if st.button("⭐ Add to Wishlist", key=f"add_wish_{isbn_input}"):
                    st.session_state['wishlist'] = pd.concat(
                        [st.session_state['wishlist'], pd.DataFrame([{'ISBN': isbn_input, **meta}])],
                        ignore_index=True
                    )
                    sync_session('wishlist')
                    st.experimental_rerun()

st.divider()


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
        sync_session("library")   # or sync_session("wishlist")
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

