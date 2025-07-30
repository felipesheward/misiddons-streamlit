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

# ---------- Optional barcode support ----------
try:
    from pyzbar.pyzbar import decode as zbar_decode
except ImportError:
    zbar_decode = None

# ---------- Streamlit config ----------
st.set_page_config(page_title="Misiddons Book Database", layout="wide")
st.markdown(
    """
    <style>
    [data-testid=column]:not(:last-child){margin-right:1rem;}
    .stButton > button{width:100%;}
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

# ---------- Persistence helpers ----------

def load_db(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype={"ISBN": str})
    except FileNotFoundError:
        df = pd.DataFrame(
            columns=["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating"]
        )
    df["Rating"] = pd.to_numeric(df.get("Rating", pd.NA), errors="coerce")
    return df


def save_db(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    out["ISBN"] = out["ISBN"].astype(str)
    out.to_csv(path, index=False)


def sync_session(name: str) -> None:
    """Persist the named DataFrame to its CSV."""
    if name == "library":
        save_db(st.session_state[name], BOOK_DB)
    elif name == "wishlist":
        save_db(st.session_state[name], WISHLIST_DB)
    else:
        raise ValueError(name)

# ---------- Barcode helper ----------

def scan_barcode(image: Image.Image) -> str | None:
    if zbar_decode is None:
        return None
    img = ImageOps.exif_transpose(image).convert("RGB")
    res = zbar_decode(img) or zbar_decode(img.resize((img.width*2, img.height*2)))
    return res[0].data.decode("utf-8") if res else None

# ---------- Fetch book details ----------

def _clip(text: str | None, n: int = 300) -> str:
    """Trim text to n characters, appending ellipsis if needed; default if empty."""
    s = (text or "").strip()
    if not s:
        return "No description available."
    return s[:n] + ("..." if len(s) > n else "")


def _norm_lang(code: str | None) -> str:
    """Normalize language code to uppercase, or return 'Unknown'."""
    return (code or "").upper() or "Unknown"


def fetch_from_google(isbn: str) -> dict | None:
    """Fetch book info from Google Books API, with thumbnail fallback."""
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"isbn:{isbn}"},
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        r.raise_for_status()
    except Exception:
        return None
    data = r.json()
    items = data.get("items", [])
    if not items:
        return None
    info = items[0].get("volumeInfo", {})
    # Thumbnail: try thumbnail, then smallThumbnail
    img_links = info.get("imageLinks", {})
    thumb = img_links.get("thumbnail") or img_links.get("smallThumbnail") or ""
    # Description: prefer full description over snippet
    desc = info.get("description") or items[0].get("searchInfo", {}).get("textSnippet") or ""
    return {
        "Title": info.get("title", "Unknown Title"),
        "Author": ", ".join(info.get("authors", ["Unknown Author"])),
        "Genre": ", ".join(info.get("categories", ["Unknown Genre"])),
        "Language": _norm_lang(info.get("language")),
        "Thumbnail": thumb,
        "Description": _clip(desc),
        "Rating": pd.NA,
    }


def fetch_from_openlibrary(isbn: str) -> dict | None:
    """Fetch book info from OpenLibrary API, with cover and description handling."""
    try:
        r = requests.get(f"https://openlibrary.org/isbn/{isbn}.json", timeout=12)
        if r.status_code != 200:
            return None
        j = r.json()
    except Exception:
        return None
    title = j.get("title", "Unknown Title")
    # Authors
    authors = []
    for a in j.get("authors", []):
        try:
            ar = requests.get(f"https://openlibrary.org{a['key']}.json", timeout=6)
            if ar.ok:
                authors.append(ar.json().get("name", "Unknown Author"))
        except Exception:
            continue
    # Cover fallback
    cover_id = j.get("covers", [None])[0]
    thumb = (
        f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg"
        if cover_id
        else ""
    )
    # Description
    desc = j.get("description", "")
    if isinstance(desc, dict):
        desc = desc.get("value", "")
    # Language
    lang = "Unknown"
    if j.get("languages"):
        code = j["languages"][0].get("key", "").split("/")[-1]
        lang = code.upper() or "Unknown"
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
    """
    Unified fetch: try Google, then OpenLibrary;
    add fallback cover via ISBN; ensure description.
    """
    key = isbn.replace("-", "").strip()
    details = None
    # 1) Google Books
    try:
        details = fetch_from_google(key)
    except Exception:
        details = None
    # 2) OpenLibrary fallback
    if not details:
        try:
            details = fetch_from_openlibrary(key)
        except Exception:
            details = None
    # 3) Fallbacks for thumbnail & description
    if details:
        # If no thumbnail yet, use OpenLibrary cover by ISBN
        if not details.get("Thumbnail"):
            details["Thumbnail"] = f"https://covers.openlibrary.org/b/isbn/{key}-L.jpg"
        # Ensure description is non-empty
        desc = details.get("Description", "") or ""
        details["Description"] = desc.strip() or "No description available."
    return details

# ---------- Session state init ----------
if "library" not in st.session_state:
    st.session_state["library"] = load_db(BOOK_DB)
if "wishlist" not in st.session_state:
    st.session_state["wishlist"] = load_db(WISHLIST_DB)

library_df: pd.DataFrame = st.session_state["library"]
wishlist_df: pd.DataFrame = st.session_state["wishlist"]

# ---------- UI ----------
st.title("📚 Misiddons Book Database")

# --- Add / Scan ---
tab1, tab2 = st.tabs(["📷 Scan barcode","✍️ Enter ISBN"])
with tab1:
    f = st.file_uploader("Upload barcode image", type=["jpg","jpeg","png"], key="scan")
    isbn_scanned = None
    if f:
        img = Image.open(f)
        isbn_scanned = scan_barcode(img)
        if isbn_scanned:
            st.success(f"ISBN detected: {isbn_scanned}")
            st.image(img, caption=isbn_scanned, width=160)
        else:
            st.error("No barcode detected.")
with tab2:
    isbn_scanned = isbn_scanned or None
    manual = st.text_input("ISBN", key="manual")
    isbn_input = (manual or isbn_scanned or "").strip()

if isbn_input:
    if isbn_input in library_df["ISBN"].values:
        st.warning("Book already in library")
    elif isbn_input in wishlist_df["ISBN"].values:
        st.warning("Book already on wishlist")
    else:
        with st.spinner("Fetching details…"):
            meta = fetch_book_details(isbn_input) or {}
        if not meta.get("Title"):
            st.error("Details not found – fill manually.")
            meta["Title"] = st.text_input("Title *required*")
            meta["Author"] = st.text_input("Author", value="Unknown")
            meta["Genre"] = st.text_input("Genre", value="Unknown")
            meta["Language"] = st.text_input("Language", value="Unknown")
            meta["Thumbnail"] = ""
            meta["Description"] = st.text_area("Description")
            meta["Rating"] = pd.NA
        if meta.get("Title"):
            c1, c2 = st.columns([1,3])
            if meta.get("Thumbnail",""
                ).startswith("http"):
                c1.image(meta["Thumbnail"], width=120)
            with c2:
                st.markdown(f"### {meta['Title']}")
                st.caption(meta['Author'])
                st.write(meta['Description'])
            b1, b2 = st.columns(2)
            with b1:
                if st.button("➕ Add to Library", key=f"add_lib_{isbn_input}"):
                    st.session_state["library"] = pd.concat(
                        [st.session_state["library"], pd.DataFrame([{"ISBN": isbn_input, **meta}])],
                        ignore_index=True
                    )
                    sync_session("library")
            with b2:
                if st.button("⭐ Add to Wishlist", key=f"add_wish_{isbn_input}"):
                    st.session_state["wishlist"] = pd.concat(
                        [st.session_state["wishlist"], pd.DataFrame([{"ISBN": isbn_input, **meta}])],
                        ignore_index=True
                    )
                    sync_session("wishlist")

st.divider()

# --- Search & Rate ---
st.subheader("🔎 Search & Rate Library")
query = st.text_input("Search by title, author, or ISBN")
view_df = library_df
if query:
    q = query.lower()
    mask = (
        library_df["Title"].str.contains(q, case=False, na=False) |
        library_df["Author"].str.contains(q, case=False, na=False) |
        library_df["ISBN"].str.contains(q, case=False, na=False)
    )
    view_df = library_df[mask]
if view_df.empty:
    st.info("No books found")
else:
    for i, row in enumerate(view_df.sort_values("Title").itertuples(), start=1):
        with st.expander(f"{row.Title} – {row.Author}"):
            if isinstance(row.Thumbnail, str) and row.Thumbnail.startswith("http"):
                st.image(row.Thumbnail, width=100)
            st.write(row.Description)
            curr = int(row.Rating) if pd.notna(row.Rating) else 0
            new = st.slider(
                "Rate this book",
                0, 5, curr,
                key=f"rate_{row.ISBN}_{i}"
            )
            if new != curr:
                idx = library_df.index[library_df["ISBN"] == row.ISBN][0]
                library_df.at[idx, "Rating"] = new
                sync_session("library")
                st.success("Rating saved")

st.divider()

# --- Tables ---
st.subheader("My Library")
if not library_df.empty:
    st.dataframe(library_df.iloc[::-1].reset_index(drop=True))
else:
    st.info("Library empty")

st.subheader("My Wishlist")
if not wishlist_df.empty:
    st.dataframe(wishlist_df.iloc[::-1].reset_index(drop=True))
else:
    st.info("Wishlist empty")

# --- Summary ---
st.subheader("Library Summary")
if not library_df.empty:
    st.metric("Total books", len(library_df))
    st.write("Languages:")
    for lang, cnt in library_df["Language"].value_counts().items():
        st.write(f"- {lang}: {cnt}")
# --- End ---
