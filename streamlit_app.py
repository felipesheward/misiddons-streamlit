# (updated) streamlit_app.py

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Form + Scanner + Gallery)
"""

from __future__ import annotations

import random
import pandas as pd
import requests
import streamlit as st
from pathlib import Path
from PIL import Image

# Optional barcode support
try:
    from pyzbar.pyzbar import decode as zbar_decode
except Exception:
    zbar_decode = None

# ---------- CONFIG ----------
st.set_page_config(page_title="Misiddons Book Database", layout="wide")

DEFAULT_SHEET_ID = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"
SPREADSHEET_ID = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)

# ... (your auth, gspread helpers, and other utility functions remain unchanged) ...

# ---------- Helpers ----------
def _normalize_isbn(isbn: str) -> str:
    return (isbn or "").replace("-", "").strip()

def _clip(text: str | None, n: int = 300) -> str:
    text = (text or "").strip()
    return text[:n] + ("..." if len(text) > n else "") if text else "No description available."

def _cover_or_placeholder(url: str, title: str):
    """
    Returns (img_url, caption) using placeholder if the URL is empty/bad.
    """
    title = (title or "Untitled").strip()
    if not url or url.lower() in {"nan", "none"}:
        # Tiny SVG placeholder (data URI) — keeps layout consistent
        svg = f'''data:image/svg+xml;utf8,
<svg xmlns="http://www.w3.org/2000/svg" width="600" height="900">
  <rect width="100%" height="100%" fill="#f0f0f0"/>
  <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" font-family="sans-serif" font-size="28" fill="#999">{title}</text>
</svg>'''
        return svg, title
    return url, title

@st.cache_data(show_spinner=False)
def load_data(sheet_name: str) -> pd.DataFrame:
    # Your existing implementation to read Google Sheet...
    # return pd.DataFrame([...])
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        # ... existing auth code ...
    except Exception:
        pass
    # Placeholder to avoid NameError if secrets not set during preview
    return pd.DataFrame(columns=["Title", "Author", "Thumbnail", "Genre", "ISBN", "Description"])

# ---------- UI LAYOUT ----------
st.title("📚 Misiddons Book Database")

tabs = st.tabs(["Library", "Wishlist", "Statistics", "Recommendations"])

# ===================== Library (UPDATED: 3-per-row gallery, mobile-safe) =====================
with tabs[0]:
    st.header("My Library")

    library_df = load_data("Library")
    if library_df.empty:
        st.info("Your library is empty. Add a book to get started!")
    else:
        # Ensure expected columns exist
        for c in ["Title", "Author", "Thumbnail", "Genre", "ISBN"]:
            if c not in library_df.columns:
                library_df[c] = ""

        # Search box
        search_lib = st.text_input(
            "🔎 Search My Library...",
            placeholder="Search titles, authors, or genres...",
            key="lib_search",
        )

        lib_df_display = library_df.copy()
        if search_lib:
            lib_df_display = lib_df_display[
                lib_df_display.apply(
                    lambda row: row.astype(str).str.contains(search_lib, case=False, na=False).any(),
                    axis=1,
                )
            ]

        # --- 3 thumbnails per row everywhere (mobile-safe via CSS Grid) ---
        st.metric("Books shown", len(lib_df_display))

        if lib_df_display.empty:
            st.info("No matches.")
        else:
            items = []
            for _, row in lib_df_display.iterrows():
                img_url, _ = _cover_or_placeholder(
                    str(row.get("Thumbnail", "")),
                    str(row.get("Title", "")),
                )
                title  = (row.get("Title")  or "Untitled").strip()
                author = (row.get("Author") or "").strip()
                cap = f"{title} — {author}" if author else title

                items.append(f"""
                <figure class="cover-card">
                  <img src="{img_url}" alt="{cap}" loading="lazy">
                  <figcaption>{cap}</figcaption>
                </figure>
                """)

            html = f"""
            <style>
              .cover-grid {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 0.75rem;
                width: 100%;
              }}
              .cover-card {{
                margin: 0;
              }}
              .cover-card img {{
                width: 100%;
                height: auto;
                display: block;
                border-radius: 0.5rem;
              }}
              .cover-card figcaption {{
                font-size: 0.8rem;
                line-height: 1.2;
                margin-top: 0.25rem;
                text-align: center;
                word-break: break-word;
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                overflow: hidden;
              }}
            </style>
            <div class="cover-grid">
              {''.join(items)}
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)

# ===================== Wishlist (unchanged; optional to mirror gallery) =====================
with tabs[1]:
    st.header("My Wishlist")
    wishlist_df = load_data("Wishlist")
    if wishlist_df.empty:
        st.info("Your wishlist is empty.")
    else:
        st.dataframe(wishlist_df, use_container_width=True, hide_index=True)

# ===================== Statistics (unchanged) =====================
with tabs[2]:
    st.header("Statistics")
    lib = load_data("Library")
    wish = load_data("Wishlist")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total in Library", len(lib))
    col2.metric("Total in Wishlist", len(wish))
    col3.metric("Unique Authors", lib["Author"].nunique() if not lib.empty and "Author" in lib else 0)


# ===================== Recommendations (existing logic retained) =====================
with tabs[3]:
    st.header("Recommendations")
    library_df = load_data("Library")
    wishlist_df = load_data("Wishlist")

    owned_titles = set((library_df.get("Title") or pd.Series(dtype=str)).astype(str).str.lower())
    owned_isbns  = set((_normalize_isbn(x) for x in (library_df.get("ISBN") or pd.Series(dtype=str)).astype(str)))

    # Example: surprise me = 4 random unseen picks from your existing authors
    authors = []
    if not library_df.empty and "Author" in library_df:
        authors = (
            library_df["Author"].dropna()
            .astype(str)
            .str.split(",")
            .explode()
            .str.strip()
            .replace({"": None})
            .dropna()
            .unique()
            .tolist()
        )
        authors = sorted(set(authors), key=lambda s: s.lower())

    mode = st.radio("Recommendation mode:", ["Surprise me (4 random unseen)", "By author"], horizontal=True)

    def _pick_random_from_author(author: str, k: int = 4):
        # Placeholder demo: in your real code, call your API fetchers here
        pool = [
            {"title": f"Sample Book {i} by {author}", "isbn": f"000000000{i}"} for i in range(1, 20)
        ]
        random.shuffle(pool)
        picks = []
        for item in pool:
            title = (item.get("title") or "").strip()
            isbn = _normalize_isbn(item.get("isbn", ""))
            if (title.lower() in owned_titles) or (isbn and isbn in owned_isbns):
                continue
            picks.append(item)
            if len(picks) == k:
                break
        return picks

    if mode == "By author":
        if authors:
            selected_author = st.selectbox("Find books by authors you've read:", authors)
        else:
            selected_author = st.text_input("Type an author to get recommendations:")

        if selected_author:
            recs = _pick_random_from_author(selected_author, 4)
            if not recs:
                st.info("No unseen picks found.")
            else:
                for item in recs:
                    st.write("•", item["title"])
    else:
        # Surprise me: rotate through up to 4 authors you have and pick 1 each
        if not authors:
            st.info("Add a few books first to seed your authors.")
        else:
            random_authors = random.sample(authors, k=min(4, len(authors)))
            all_recs = []
            for a in random_authors:
                all_recs.extend(_pick_random_from_author(a, 1))
            if not all_recs:
                st.info("No unseen picks right now.")
            else:
                st.subheader("Surprise me 🎲")
                for item in all_recs:
                    st.write("•", item["title"])

# ---------- (Optional) Admin / Data Quality tools below this line ----------
# Keep any additional pages/sections you had here unchanged.
# If you want the Wishlist to also show as a 3-per-row gallery,
# mirror the Library's CSS-grid block and replace the dataframe display.

if __name__ == "__main__":
    # Streamlit runs via `streamlit run`, so main guard is optional.
    pass
