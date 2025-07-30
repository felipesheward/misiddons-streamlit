#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app
"""


from __future__ import annotations
import subprocess
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

# ---------- Persistence helpers ----------

def load_db(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype={"ISBN": str})
    except FileNotFoundError:
        df = pd.DataFrame(columns=["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating"])
    df["Rating"] = pd.to_numeric(df.get("Rating", pd.NA), errors="coerce")
    return df


def save_db(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    out["ISBN"] = out["ISBN"].astype(str)
    out.to_csv(path, index=False)


def commit_changes() -> None:
    """Commit and push CSV updates back to GitHub."""
    subprocess.run(["git", "config", "--global", "user.name", "streamlit-bot"], check=True)
    subprocess.run(["git", "config", "--global", "user.email", "bot@streamlit.app"], check=True)
    subprocess.run(["git", "add", str(BOOK_DB), str(WISHLIST_DB)], check=True)
    subprocess.run(["git", "commit", "-m", "Update book data via Streamlit app"], check=True)
    subprocess.run(["git", "push"], check=True)


def sync_session(name: str) -> None:
    """Persist the named DataFrame and push to GitHub."""
    if name == 'library':
        save_db(st.session_state[name], BOOK_DB)
    elif name == 'wishlist':
        save_db(st.session_state[name], WISHLIST_DB)
    else:
        raise ValueError(f"Unknown session key: {name}")
    try:
        commit_changes()
    except Exception as e:
        st.warning(f"Git commit failed: {e}")

# ---------- Barcode helper ----------

def scan_barcode(image: Image.Image) -> str | None:
    if not zbar_decode:
        return None
    img = ImageOps.exif_transpose(image).convert("RGB")
    res = zbar_decode(img) or zbar_decode(img.resize((img.width*2, img.height*2)))
    return res[0].data.decode("utf-8") if res else None

# ---------- Book info fetchers ----------

def _clip(text: str | None, n: int=300) -> str:
    s = (text or "").strip()
    return s[:n] + ("..." if len(s)>n else "") if s else "No description."

def _norm_lang(code: str | None) -> str:
    return (code or "").upper() or "Unknown"

def fetch_from_google(isbn: str) -> dict | None:
    r = requests.get("https://www.googleapis.com/books/v1/volumes", params={"q":f"isbn:{isbn}"}, timeout=10)
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
    r = requests.get(f"https://openlibrary.org/isbn/{isbn}.json", timeout=10)
    if r.status_code != 200: return None
    j = r.json()
    authors = []
    for a in j.get("authors",[]):
        ar = requests.get(f"https://openlibrary.org{a['key']}.json", timeout=5)
        if ar.ok: authors.append(ar.json().get("name","Unknown Author"))
    thumb = f"https://covers.openlibrary.org/b/id/{j.get('covers',[None])[0]}-M.jpg" if j.get("covers") else ""
    desc = j.get("description","")
    if isinstance(desc,dict): desc = desc.get("value","")
    lang = "Unknown"
    if j.get("languages"): lang = j["languages"][0].get("key","").split("/")[-1].upper() or "Unknown"
    return {"Title":j.get("title","Unknown Title"),"Author":", ".join(authors) or "Unknown Author","Genre":"Unknown","Language":lang,"Thumbnail":thumb,"Description":_clip(desc),"Rating":pd.NA}

def fetch_book_details(isbn: str) -> dict | None:
    key = isbn.replace("-","").strip()
    return fetch_from_google(key) or fetch_from_openlibrary(key)

# ---------- Recommendations ----------
@st.cache_data(ttl=3600, show_spinner=False)
def get_recommendations_by_author(author:str, max_results:int=5)->list[dict]:
    out=[]
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q":f'inauthor:"{author}"',"maxResults":max_results}, timeout=8
        )
        if r.ok:
            for item in r.json().get("items",[]):
                info=item.get("volumeInfo",{})
                out.append({
                    "Title":info.get("title","Unknown"),
                    "Authors":", ".join(info.get("authors",["Unknown Author"])),
                    "Year":info.get("publishedDate","").split("-")[0] or "Unknown",
                    "Thumbnail":info.get("imageLinks",{}).get("thumbnail",""),
                })
    except:
        pass
    return out[:max_results]

# ---------- Session‑state init ----------
if 'library' not in st.session_state:
    st.session_state['library'] = load_db(BOOK_DB)
if 'wishlist' not in st.session_state:
    st.session_state['wishlist'] = load_db(WISHLIST_DB)

library_df: pd.DataFrame = st.session_state['library']
wishlist_df: pd.DataFrame = st.session_state['wishlist']

# ---------- UI ----------
st.title("📚 Misiddons Book Database")

# --- Add / Scan ---
tab1, tab2 = st.tabs(['📷 Scan barcode','✍️ Enter ISBN'])
with tab1:
    f = st.file_uploader('Upload barcode image', type=['jpg','jpeg','png'], key='scan')
    isbn_scanned=None
    if f:
        img=Image.open(f)
        isbn_scanned=scan_barcode(img)
        if isbn_scanned:
            st.success(f'ISBN detected: {isbn_scanned}')
            st.image(img,caption=isbn_scanned,width=160)
        else:
            st.error('No barcode detected.')
with tab2:
    isbn_scanned = isbn_scanned or None
    manual = st.text_input('ISBN',key='manual')
    isbn_input=(manual or isbn_scanned or '').strip()

if isbn_input:
    if isbn_input in library_df['ISBN'].values:
        st.warning('Book already in library')
    elif isbn_input in wishlist_df['ISBN'].values:
        st.warning('Book already on wishlist')
    else:
        with st.spinner('Fetching details…'):
            meta=fetch_book_details(isbn_input) or {}
        if not meta.get('Title'):
            st.error('Details not found–provide manually')
            meta['Title']=st.text_input('Title*')
            meta['Author']=st.text_input('Author',value='Unknown')
            meta['Genre']=st.text_input('Genre',value='Unknown')
            meta['Language']=st.text_input('Language',value='Unknown')
            meta['Thumbnail']=''
            meta['Description']=st.text_area('Description')
            meta['Rating']=pd.NA
        if meta.get('Title'):
            c1,c2 = st.columns([1,3])
            if meta.get('Thumbnail','').startswith('http'):
                c1.image(meta['Thumbnail'],width=120)
            with c2:
                st.markdown(f"### {meta['Title']}")
                st.caption(meta['Author'])
                st.write(meta['Description'])
            b1,b2=st.columns(2)
            with b1:
                if st.button('➕ Add to Library',key=f'add_lib_{isbn_input}'):
                    st.session_state['library']=pd.concat([
                        st.session_state['library'],pd.DataFrame([{'ISBN':isbn_input,**meta}])
                    ],ignore_index=True)
                    sync_session('library')
                    st.experimental_rerun()
            with b2:
                if st.button('⭐ Add to Wishlist',key=f'add_wish_{isbn_input}'):
                    st.session_state['wishlist']=pd.concat([
                        st.session_state['wishlist'],pd.DataFrame([{'ISBN':isbn_input,**meta}])
                    ],ignore_index=True)
                    sync_session('wishlist')
                    st.experimental_rerun()

st.divider()

# --- Search & Rate Library ---
st.subheader('🔎 Search & Rate your Library')
query = st.text_input('Search by title, author, or ISBN')
view_df = library_df
if query:
    q=query.lower()
    mask=(
        library_df['Title'].str.contains(q,case=False,na=False)|
        library_df['Author'].str.contains(q,case=False,na=False)|
        library_df['ISBN'].str.contains(q,case=False,na=False)
    )
    view_df=library_df[mask]
if view_df.empty:
    st.info('No books found')
else:
    for i,row in enumerate(view_df.sort_values('Title').itertuples(),start=1):
        with st.expander(f"{row.Title} – {row.Author}"):
            if isinstance(row.Thumbnail,str) and row.Thumbnail.startswith('http'):
                st.image(row.Thumbnail,width=100)
            st.write(row.Description)
            curr=int(row.Rating) if pd.notna(row.Rating) else 0
            new=st.slider('Rate this book',0,5,curr,key=f'rate_slider_{row.ISBN}_{i}')
            if new!=curr:
                idx=library_df.index[library_df['ISBN']==row.ISBN][0]
                library_df.at[idx,'Rating']=new
                sync_session('library')
                st.success('Rating saved')

st.divider()

# --- Library & Wishlist Tables ---
st.subheader('My Library')
if not library_df.empty:
    st.dataframe(library_df.iloc[::-1].reset_index(drop=True))
else:
    st.info('Your library is empty')

st.subheader('My Wishlist')
if not wishlist_df.empty:
    st.dataframe(wishlist_df.iloc[::-1].reset_index(drop=True))
else:
    st.info('Your wishlist is empty')

st.divider()

# --- Summary & Recommendations ---
st.subheader('Library Summary')
if not library_df.empty:
    st.metric('Total books',len(library_df))
    st.write('Languages:')
    for lang,c in library_df['Language'].value_counts().items():
        st.write(f"- {lang}: {c}")

# Top-rated books
top_rated=(
    library_df.dropna(subset=['Rating'])
              .sort_values(['Rating','Title'],ascending=[False,True])
              .head(5)
)[['Title','Author','Rating']]
if not top_rated.empty:
    st.subheader('Top Rated Books')
    for row in top_rated.itertuples(index=False):
        st.write(f"{row.Title} by {row.Author} — {row.Rating}/5")

# Recommendations by author
if not library_df.empty:
    st.subheader('Recommended by Your Authors')
    authors=sorted({a.strip() for auth in library_df['Author'].str.split(',') for a in auth})
    fav=st.selectbox('Select author',authors)
    if st.button('Get Recommendations'):
        recs=get_recommendations_by_author(fav)
        if recs:
            for rec in recs:
                st.write(f"* {rec['Title']} ({rec['Year']})")
        else:
            st.info('No recs found')

# --- End of app ---
