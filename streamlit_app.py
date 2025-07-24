#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app
"""

import random
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageOps

# -------- Optional barcode support --------
try:
    from pyzbar.pyzbar import decode as zbar_decode
except Exception:
    zbar_decode = None  # pyzbar/libzbar not available

# -------- Streamlit config --------
st.set_page_config(page_title="Misiddons Book Database", layout="wide")

# Base + card + horizontal scroll CSS
st.markdown("""
<style>
[data-testid=column]:not(:last-child){margin-right:1rem;}
.stButton > button{width:100%;}

/* Vertical cards (older sections) */
.card{
  border:1px solid #e5e5e5;border-radius:12px;padding:16px;
  box-shadow:0 2px 8px rgb(0 0 0 / 6%);margin-bottom:18px;background:white;
}
.card h4{margin:0 0 6px 0;font-size:1.05rem;}
.badge{
  display:inline-block;padding:2px 8px;font-size:.75rem;
  background:#f0f2f6;border-radius:6px;margin-right:6px;
}
.stars{color:#ffb400;font-size:1.1rem;}

/* Horizontal scroll strip */
.hscroll{display:flex;overflow-x:auto;gap:16px;padding:8px 0 4px 0;scrollbar-width:thin;}
.hscroll::-webkit-scrollbar{height:6px;}
.hscroll::-webkit-scrollbar-thumb{background:#bbb;border-radius:3px;}

.card-mini{
  flex:0 0 240px;
  border:1px solid #e5e5e5;border-radius:12px;padding:12px;
  box-shadow:0 2px 8px rgb(0 0 0 / 6%);background:white;
}
.card-mini h5{margin:0 0 6px 0;font-size:.95rem;}
.cover-thumb{width:100%;height:160px;object-fit:cover;border-radius:8px;margin-bottom:8px;}
.stars-mini{color:#ffb400;font-size:1rem;margin-bottom:6px;display:block;}
.badge-mini{display:inline-block;padding:2px 6px;font-size:.7rem;background:#f0f2f6;border-radius:6px;margin-bottom:6px;}
</style>
""", unsafe_allow_html=True)

# -------- Paths --------
BASE = Path(__file__).parent
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(exist_ok=True)

BOOK_DB = DATA_DIR / "books_database.csv"
WISHLIST_DB = DATA_DIR / "wishlist_database.csv"

# -------- Helpers --------
def load_db(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype={"ISBN": str})
    except FileNotFoundError:
        df = pd.DataFrame(columns=[
            "ISBN","Title","Author","Genre","Language",
            "Thumbnail","Description","Rating"
        ])
    if "Rating" not in df.columns:
        df["Rating"] = pd.NA
    else:
        df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    return df

def save_db(df: pd.DataFrame, path: Path) -> pd.DataFrame:
    df = df.copy()
    df["ISBN"] = df["ISBN"].astype(str)
    if "Rating" not in df.columns:
        df["Rating"] = pd.NA
    df.to_csv(path, index=False)
    return df

def scan_barcode(image: Image.Image) -> str | None:
    if zbar_decode is None:
        return None
    img = ImageOps.exif_transpose(image).convert("RGB")
    res = zbar_decode(img)
    if not res:
        big = img.resize((img.width * 2, img.height * 2))
        res = zbar_decode(big)
    return res[0].data.decode("utf-8") if res else None

def _clip(text: str | None, n: int = 300) -> str:
    if not text:
        return ""
    text = text.strip()
    return text[:n] + ("..." if len(text) > n else "")

def _norm_lang(code: str | None) -> str:
    return (code or "").upper() or "Unknown"

# -------- Book detail fetchers --------
def fetch_from_google(isbn: str) -> dict | None:
    url = "https://www.googleapis.com/books/v1/volumes"
    r = requests.get(url, params={"q": f"isbn:{isbn}"}, timeout=12,
                     headers={"User-Agent": "misiddons/1.0"})
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        return None
    item = items[0]
    info = item.get("volumeInfo", {})
    desc = (info.get("description")
            or item.get("searchInfo", {}).get("textSnippet")
            or info.get("subtitle"))
    return {
        "Title": info.get("title", "Unknown Title"),
        "Author": ", ".join(info.get("authors", ["Unknown Author"])),
        "Genre": ", ".join(info.get("categories", ["Unknown Genre"])),
        "Language": _norm_lang(info.get("language")),
        "Thumbnail": info.get("imageLinks", {}).get("thumbnail", ""),
        "Description": _clip(desc),
        "Rating": pd.NA,
    }

def fetch_from_openlibrary(isbn: str) -> dict | None:
    r = requests.get(f"https://openlibrary.org/isbn/{isbn}.json", timeout=12)
    if r.status_code != 200:
        return None
    j = r.json()

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
        "Title": j.get("title", "Unknown Title"),
        "Author": ", ".join(authors) if authors else "Unknown Author",
        "Genre": "Unknown",
        "Language": lang,
        "Thumbnail": thumb,
        "Description": _clip(desc),
        "Rating": pd.NA,
    }

def fetch_book_details(isbn: str) -> dict | None:
    isbn = isbn.replace("-", "").strip()
    try:
        d = fetch_from_google(isbn)
        if d:
            return d
    except Exception:
        pass
    try:
        return fetch_from_openlibrary(isbn)
    except Exception:
        return None

# -------- Recommendations --------
@st.cache_data(ttl=3600, show_spinner=False)
def get_recommendations_by_author(author: str, max_results: int = 5) -> list[dict]:
    def clip(t, n=300):
        if not t:
            return ""
        t = t.strip()
        return t[:n] + ("..." if len(t) > n else "")

    recs: list[dict] = []

    # Google Books
    try:
        url = "https://www.googleapis.com/books/v1/volumes"
        params = {"q": f'inauthor:"{author}"', "maxResults": max_results}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        for item in r.json().get("items", []):
            info = item.get("volumeInfo", {})
            desc = (info.get("description")
                    or item.get("searchInfo", {}).get("textSnippet")
                    or info.get("subtitle"))
            recs.append({
                "Title": info.get("title", "Unknown Title"),
                "Authors": ", ".join(info.get("authors", ["Unknown Author"])),
                "Year": info.get("publishedDate", "").split("-")[0] or "Unknown",
                "Rating": info.get("averageRating", "N/A"),
                "Thumbnail": info.get("imageLinks", {}).get("thumbnail", ""),
                "Description": clip(desc)
            })
    except Exception:
        pass

    if len(recs) >= max_results:
        return recs[:max_results]

    # OpenLibrary fallback
    try:
        url = f"https://openlibrary.org/search.json?author={quote(author)}&limit={max_results}"
        r = requests.get(url, timeout=8)
        if r.ok:
            for d in r.json().get("docs", []):
                cover_id = d.get("cover_i")
                thumb = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else ""
                desc = d.get("first_sentence") or d.get("subtitle") or ""
                recs.append({
                    "Title": d.get("title", "Unknown Title"),
                    "Authors": ", ".join(d.get("author_name", ["Unknown Author"])),
                    "Year": str(d.get("first_publish_year", "Unknown")),
                    "Rating": "N/A",
                    "Thumbnail": thumb,
                    "Description": clip(desc)
                })
    except Exception:
        pass

    return recs[:max_results]

# -------- Session state --------
if "library" not in st.session_state:
    st.session_state["library"] = load_db(BOOK_DB)
if "wishlist" not in st.session_state:
    st.session_state["wishlist"] = load_db(WISHLIST_DB)

library_df = st.session_state["library"].copy()
wishlist_df = st.session_state["wishlist"].copy()

# -------- UI --------
st.title("Misiddons Book Database")

with st.expander("Maintenance"):
    if st.button("Clear cached API calls"):
        st.cache_data.clear()
        st.success("Cache cleared. Fetch again.")

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
                        "Title": manual_title, "Author": "Unknown",
                        "Genre": "Unknown", "Language": "Unknown",
                        "Thumbnail": "", "Description": "", "Rating": pd.NA
                    }

            if details:
                c1, c2 = st.columns([1,3])
                if details.get("Thumbnail","").startswith("http"):
                    c1.image(details["Thumbnail"], width=120)
                with c2:
                    st.markdown(f"### {details['Title']}")
                    st.write(f"*{details['Author']}*")
                    st.write(details["Description"] or "No description available.")

                b1, b2 = st.columns(2)
                with b1:
                    if st.button("Add to Library", key=f"add_lib_{isbn}"):
                        library_df = pd.concat(
                            [library_df, pd.DataFrame([{"ISBN": isbn, **details}])],
                            ignore_index=True
                        )
                        st.session_state["library"] = save_db(library_df, BOOK_DB)
                        st.success("Added to Library!")
                with b2:
                    if st.button("Add to Wishlist", key=f"add_wish_{isbn}"):
                        wishlist_df = pd.concat(
                            [wishlist_df, pd.DataFrame([{"ISBN": isbn, **details}])],
                            ignore_index=True
                        )
                        st.session_state["wishlist"] = save_db(wishlist_df, WISHLIST_DB)
                        st.success("Added to Wishlist!")
    else:
        if zbar_decode is None:
            st.error("Barcode module unavailable. Enter ISBN manually below.")
        else:
            st.error("No barcode detected. Enter ISBN manually below.")
        manual_isbn = st.text_input("ISBN (manual):", "")
        if manual_isbn:
            with st.spinner("Fetching book details..."):
                details = fetch_book_details(manual_isbn)
            if details:
                c1, c2 = st.columns([1,3])
                if details.get("Thumbnail","").startswith("http"):
                    c1.image(details["Thumbnail"], width=120)
                with c2:
                    st.markdown(f"### {details['Title']}")
                    st.write(f"*{details['Author']}*")
                    st.write(details["Description"] or "No description available.")
                b1, b2 = st.columns(2)
                with b1:
                    if st.button("Add to Library", key=f"add_lib_manual_{manual_isbn}"):
                        library_df = pd.concat(
                            [library_df, pd.DataFrame([{"ISBN": manual_isbn, **details}])],
                            ignore_index=True
                        )
                        st.session_state["library"] = save_db(library_df, BOOK_DB)
                        st.success("Added to Library!")
                with b2:
                    if st.button("Add to Wishlist", key=f"add_wish_manual_{manual_isbn}"):
                        wishlist_df = pd.concat(
                            [wishlist_df, pd.DataFrame([{"ISBN": manual_isbn, **details}])],
                            ignore_index=True
                        )
                        st.session_state["wishlist"] = save_db(wishlist_df, WISHLIST_DB)
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
    if ("rate_current_isbn" not in st.session_state or
        st.session_state["rate_current_isbn"] not in unrated["ISBN"].tolist()):
        book = unrated.sample(1, random_state=random.randint(0, 10000)).iloc[0]
        st.session_state["rate_current_isbn"] = book["ISBN"]
    else:
        book = library_df.loc[library_df["ISBN"] == st.session_state["rate_current_isbn"]].iloc[0]

    idx0 = library_df.index[library_df["ISBN"] == book["ISBN"]][0]
    st.markdown(f"**Title:** {book['Title']}  \n**Author:** {book['Author']}")
    thumb = book.get("Thumbnail","")
    if isinstance(thumb, str) and thumb.startswith("http"):
        st.image(thumb, width=150)

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
        st.session_state["library"] = save_db(library_df, BOOK_DB)
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

    st.write("#### Top 5 Authors")
    auth = (library_df["Author"]
            .str.split(",")
            .explode()
            .str.strip()
            .value_counts()
            .head(5))
    st.bar_chart(auth)

# --- Top Rated Books (horizontal scroll) ---
st.subheader("Top Rated Books")
top_rated = (library_df[library_df["Rating"].notna()]
             .sort_values("Rating", ascending=False)
             .head(10))

if top_rated.empty:
    st.info("You haven’t rated any books yet!")
else:
    cards_html = []
    for _, book in top_rated.iterrows():
        stars = "★" * int(book["Rating"]) + "☆" * (5 - int(book["Rating"]))
        thumb = book.get("Thumbnail", "")
        cover_tag = (f'<img src="{thumb}" class="cover-thumb">'
                     if isinstance(thumb, str) and thumb.startswith("http")
                     else '<div class="cover-thumb" style="background:#eee;display:flex;align-items:center;justify-content:center;font-size:.8rem;color:#777;">No cover</div>')
        desc = book.get("Description","") or "No description available."
        card = f"""
        <div class="card-mini">
          {cover_tag}
          <h5>{book['Title']}</h5>
          <span class="badge-mini">by {book['Author']}</span>
          <span class="stars-mini">{stars}</span>
          <p style="font-size:.8rem;line-height:1.25em;max-height:4.5em;overflow:hidden;">{desc}</p>
        </div>
        """
        cards_html.append(card)
    st.markdown(f'<div class="hscroll">{"".join(cards_html)}</div>', unsafe_allow_html=True)
    st.caption("Swipe/scroll horizontally ⟶")

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

    if st.button("Get recommendations", key="get_recs_btn"):
        st.cache_data.clear()

    if fav_author:
        with st.spinner(f"Fetching books by {fav_author}..."):
            recs = get_recommendations_by_author(fav_author)

        st.write(f"**Found {len(recs)} recommendations.**")

        if not recs:
            st.warning("No recommendations came back. Try another author or tap the button again.")
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
