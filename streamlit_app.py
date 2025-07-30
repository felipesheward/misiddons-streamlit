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
except Exception:   # pyzbar or libzbar missing
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
    """Load a CSV into a DataFrame (create empty one if absent)."""
    try:
        df = pd.read_csv(path, dtype={"ISBN": str})
    except FileNotFoundError:
        df = pd.DataFrame(
            columns=["ISBN","Title","Author","Genre","Language",
                     "Thumbnail","Description","Rating"]
        )
    if "Rating" not in df.columns:
        df["Rating"] = pd.NA
    else:
        df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    return df

def save_db(df: pd.DataFrame, path: Path) -> None:
    """Persist *df* to CSV."""
    df = df.copy()
    df["ISBN"] = df["ISBN"].astype(str)
    df.to_csv(path, index=False)

# ---------- Session persistence wrapper ----------
def sync_session(name: str) -> None:
    """Write the DataFrame to disk."""
    if name == "library":
        save_db(st.session_state[name], BOOK_DB)
    elif name == "wishlist":
        save_db(st.session_state[name], WISHLIST_DB)
    else:
        raise ValueError("unknown dataframe")

# ---------- Barcode ----------
def scan_barcode(image: Image.Image) -> str | None:
    if zbar_decode is None:
        return None
    img = ImageOps.exif_transpose(image).convert("RGB")
    res = zbar_decode(img)
    if not res:
        res = zbar_decode(img.resize((img.width*2, img.height*2)))
    return res[0].data.decode("utf-8") if res else None

# ---------- External book info ----------
def _clip(text: str | None, n: int=300) -> str:
    text = (text or "").strip()
    return text[:n] + ("..." if len(text) > n else "") if text else "No description."

def _norm_lang(code: str | None) -> str:
    return (code or "").upper() or "Unknown"

def fetch_from_google(isbn: str) -> dict | None:
    url = "https://www.googleapis.com/books/v1/volumes"
    r = requests.get(url, params={"q": f"isbn:{isbn}"}, timeout=12)
    if not r.ok: return None
    items = r.json().get("items", [])
    if not items: return None
    info = items[0].get("volumeInfo", {})
    desc = info.get("description") or items[0].get("searchInfo",{}).get("textSnippet")
    return {
        "Title": info.get("title","Unknown Title"),
        "Author": ", ".join(info.get("authors",["Unknown Author"])),
        "Genre": ", ".join(info.get("categories",["Unknown Genre"])),
        "Language": _norm_lang(info.get("language")),
        "Thumbnail": info.get("imageLinks",{}).get("thumbnail",""),
        "Description": _clip(desc),
        "Rating": pd.NA,
    }

def fetch_from_openlibrary(isbn: str) -> dict | None:
    r = requests.get(f"https://openlibrary.org/isbn/{isbn}.json", timeout=12)
    if r.status_code != 200: return None
    j = r.json()
    title = j.get("title","Unknown Title")
    authors=[]
    for a in j.get("authors",[]):
        ar = requests.get(f"https://openlibrary.org{a['key']}.json", timeout=6)
        if ar.ok:
            authors.append(ar.json().get("name","Unknown Author"))
    cover_id = j.get("covers",[None])[0]
    thumb = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else ""
    desc = j.get("description","")
    if isinstance(desc, dict):
        desc = desc.get("value","")
    lang = "Unknown"
    if j.get("languages"):
        lang = j["languages"][0].get("key","").split("/")[-1].upper() or "Unknown"
    return {
        "Title": title,
        "Author": ", ".join(authors) if authors else "Unknown Author",
        "Genre": "Unknown",
        "Language": lang,
        "Thumbnail": thumb,
        "Description": _clip(desc),
        "Rating": pd.NA,
    }

def fetch_book_details(isbn: str) -> dict | None:
    isbn = isbn.replace("-","").strip()
    try:
        return fetch_from_google(isbn) or fetch_from_openlibrary(isbn)
    except Exception:
        return None

# ---------- Recommendations (optional) ----------
@st.cache_data(ttl=3600, show_spinner=False)
def get_recommendations_by_author(author: str, max_results: int = 5) -> list[dict]:
    out=[]
    try:
        url="https://www.googleapis.com/books/v1/volumes"
        r=requests.get(url, params={"q":f'inauthor:"{author}"',"maxResults":max_results}, timeout=8)
        if r.ok:
            for item in r.json().get("items",[]):
                info=item.get("volumeInfo",{})
                desc=info.get("description") or item.get("searchInfo",{}).get("textSnippet")
                out.append({
                    "Title": info.get("title","Unknown"),
                    "Authors": ", ".join(info.get("authors",["Unknown Author"])),
                    "Year": info.get("publishedDate","").split("-")[0] or "Unknown",
                    "Rating": info.get("averageRating","N/A"),
                    "Thumbnail": info.get("imageLinks",{}).get("thumbnail",""),
                    "Description": _clip(desc),
                })
    except Exception:
        pass
    if len(out)>=max_results:
        return out[:max_results]

    # Fallback OpenLibrary
    try:
        url=f"https://openlibrary.org/search.json?author={quote(author)}&limit={max_results}"
        r=requests.get(url,timeout=8)
        if r.ok:
            for d in r.json().get("docs",[]):
                cover_id=d.get("cover_i")
                thumb=f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else ""
                desc=d.get("first_sentence") or d.get("subtitle") or ""
                out.append({
                    "Title": d.get("title","Unknown"),
                    "Authors": ", ".join(d.get("author_name",["Unknown Author"])),
                    "Year": str(d.get("first_publish_year","Unknown")),
                    "Rating": "N/A",
                    "Thumbnail": thumb,
                    "Description": _clip(desc),
                })
    except Exception:
        pass
    return out[:max_results]

# ---------- Session state ----------
if "library" not in st.session_state:
    st.session_state["library"] = load_db(BOOK_DB)
if "wishlist" not in st.session_state:
    st.session_state["wishlist"] = load_db(WISHLIST_DB)

library_df: pd.DataFrame = st.session_state["library"]
wishlist_df: pd.DataFrame = st.session_state["wishlist"]

# ---------- UI ----------
st.title("📚 Misiddons Book Database")

# --- Add / Scan section ---
st.subheader("Add a Book")
tab_scan, tab_man = st.tabs(["Scan barcode", "Enter ISBN manually"])

with tab_scan:
    book_file = st.file_uploader("Upload a barcode image", type=["jpg","jpeg","png"], key="scan")
    if book_file:
        img = Image.open(book_file)
        isbn = scan_barcode(img)
        if isbn:
            st.success(f"ISBN detected: {isbn}")
            st.image(img, caption=isbn, width=160)
        else:
            st.error("Barcode not detected.")
            isbn = None
else:
    isbn = None

with tab_man:
    manual_isbn = st.text_input("ISBN", key="manual_isbn")
    if manual_isbn:
        isbn = manual_isbn.strip()

def show_details_block(book:dict):
    col1,col2 = st.columns([1,3])
    if book.get("Thumbnail","").startswith("http"):
        col1.image(book["Thumbnail"], width=120)
    with col2:
        st.markdown(f"### {book['Title']}")
        st.caption(book['Author'])
        st.write(book['Description'])

if isbn:
    if isbn in library_df["ISBN"].values:
        st.warning("Book already in library.")
    elif isbn in wishlist_df["ISBN"].values:
        st.warning("Book already on wishlist.")
    else:
        with st.spinner("Fetching book details…"):
            details = fetch_book_details(isbn)
        if not details:
            st.error("Details not found. Fill fields manually.")
            details = {
                "Title": st.text_input("Title *required*"),
                "Author": st.text_input("Author"),
                "Genre": st.text_input("Genre"),
                "Language": st.text_input("Language", value="Unknown"),
                "Thumbnail": "",
                "Description": st.text_area("Description"),
                "Rating": pd.NA,
            }
        if details["Title"]:
            show_details_block(details)
            c1,c2 = st.columns(2)
            with c1:
                if st.button("➕ Add to Library"):
                    st.session_state["library"] = pd.concat(
                        [library_df, pd.DataFrame([{"ISBN": isbn, **details}])],
                        ignore_index=True
                    )
                    sync_session("library")
                    st.success("Added to library.")
                    st.experimental_rerun()
            with c2:
                if st.button("⭐ Add to Wishlist"):
                    st.session_state["wishlist"] = pd.concat(
                        [wishlist_df, pd.DataFrame([{"ISBN": isbn, **details}])],
                        ignore_index=True
                    )
                    sync_session("wishlist")
                    st.success("Added to wishlist.")
                    st.experimental_rerun()

st.divider()

# --- Search ---
st.subheader("Search your Library")
search_q = st.text_input("Search by title, author, or ISBN")
filtered_df = library_df.copy()
if search_q:
    q = search_q.lower()
    mask = (
        library_df["Title"].str.contains(q, case=False, na=False) |
        library_df["Author"].str.contains(q, case=False, na=False) |
        library_df["ISBN"].str.contains(q, case=False, na=False)
    )
    filtered_df = library_df[mask]

if filtered_df.empty:
    st.info("No books match the search.")
else:
    for _, row in filtered_df.sort_values("Title").iterrows():
        with st.expander(f"{row['Title']} – {row['Author']}"):
            show_details_block(row)
            # Rating selector
            rating = st.slider(
                f"Rate {row['Title']}", min_value=0, max_value=5, step=1,
                value=int(row["Rating"]) if pd.notna(row["Rating"]) else 0,
                key=f"rate_{row['ISBN']}"
            )
            if rating != row["Rating"]:
                idx = library_df.index[library_df["ISBN"]==row["ISBN"]][0]
                library_df.at[idx,"Rating"] = rating
                sync_session("library")
                st.success("Rating saved.")

st.divider()

# --- Wishlist ---
with st.expander("📜 Wishlist"):
    if wishlist_df.empty:
        st.info("Wishlist is empty.")
    else:
        for _, row in wishlist_df.sort_values("Title").iterrows():
            show_details_block(row)
            c1,c2,c3 = st.columns(3)
            with c1:
                if st.button("➡️ Move to Library", key=f"move_{row['ISBN']}"):
                    st.session_state["library"] = pd.concat(
                        [library_df, pd.DataFrame([row])], ignore_index=True
                    )
                    st.session_state["wishlist"] = wishlist_df[wishlist_df["ISBN"]!=row["ISBN"]]
                    sync_session("library")
                    sync_session("wishlist")
                    st.experimental_rerun()
            with c2:
                if st.button("🗑️ Remove", key=f"del_{row['ISBN']}"):
                    st.session_state["wishlist"] = wishlist_df[wishlist_df["ISBN"]!=row["ISBN"]]
                    sync_session("wishlist")
                    st.experimental_rerun()
            with c3:
                if st.button("🔍 Like this?", key=f"rec_{row['ISBN']}"):
                    st.subheader(f"Recommendations if you enjoyed {row['Author']}")
                    recs = get_recommendations_by_author(row["Author"].split(",")[0])
                    for rec in recs:
                        st.markdown(f"*{rec['Title']}* ({rec['Year']}) – {rec['Authors']}")


