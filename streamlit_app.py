#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Form + Scanner)
- Add books manually via form
- Scan barcodes from a photo to auto-fill metadata (title, author, cover, description)
- Add to Library or Wishlist
- Prevents duplicates (by ISBN or Title+Author)
"""
from __future__ import annotations

import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
from PIL import Image
from gspread.exceptions import APIError, WorksheetNotFound

# Optional barcode support
try:
    from pyzbar.pyzbar import decode as zbar_decode
except Exception:
    zbar_decode = None

# ---------- CONFIG ----------
DEFAULT_SHEET_ID = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"
SPREADSHEET_ID = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)
GOOGLE_SHEET_NAME = st.secrets.get("google_sheet_name", "database")
GOOGLE_BOOKS_KEY = st.secrets.get("google_books_api_key", None)

st.set_page_config(page_title="Misiddons Book Database", layout="wide")

# ---------- Google Sheets helpers ----------
@st.cache_resource
def connect_to_gsheets():
    if "gcp_service_account" not in st.secrets:
        st.error("gcp_service_account not found in secrets. Add your service account JSON there.")
        return None
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Failed to authorize Google Sheets: {e}")
        return None

@st.cache_data(ttl=60)
def load_data(worksheet: str) -> pd.DataFrame:
    """Fetch a worksheet into a DataFrame. Falls back to get_all_values()."""
    client_local = connect_to_gsheets()
    if not client_local:
        return pd.DataFrame()
    ss = None
    try:
        ss = client_local.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client_local.open(GOOGLE_SHEET_NAME)
        target = worksheet.strip()
        try:
            ws = ss.worksheet(target)
        except WorksheetNotFound:
            names = [w.title for w in ss.worksheets()]
            norm = {n.strip().casefold(): n for n in names}
            if target.strip().casefold() in norm:
                ws = ss.worksheet(norm[target.strip().casefold()])
            else:
                raise
        try:
            df = pd.DataFrame(ws.get_all_records())
            return df.dropna(how="all")
        except Exception:
            vals = ws.get_all_values()
            if not vals:
                return pd.DataFrame()
            header, *rows = vals
            return pd.DataFrame(rows, columns=header).dropna(how="all")
    except WorksheetNotFound:
        try:
            tabs = [w.title for w in ss.worksheets()] if ss else []
        except Exception:
            tabs = []
        st.error(f"Worksheet '{worksheet}' not found. Available tabs: {tabs}")
        return pd.DataFrame()
    except APIError as e:
        code = getattr(getattr(e, 'response', None), 'status_code', 'unknown')
        st.error(f"Google Sheets API error while loading '{worksheet}' (HTTP {code}). If 404/403, re-share the sheet with the service account and verify the ID.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error loading '{worksheet}': {type(e).__name__}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def _get_ws(tab: str):
    client = connect_to_gsheets()
    if not client:
        return None
    ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
    t = tab.strip()
    try:
        return ss.worksheet(t)
    except WorksheetNotFound:
        names = [w.title for w in ss.worksheets()]
        norm = {n.strip().casefold(): n for n in names}
        if t.casefold() in norm:
            return ss.worksheet(norm[t.casefold()])
        raise

# ---------- Sheet write helpers ----------
# Added "PublishedDate" to the list of fixed headers
EXACT_HEADERS = [
    "ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate","Date Read"
]

ISO_LANG = {
    "EN":"English","IT":"Italian","ES":"Spanish","DE":"German","FR":"French",
    "PT":"Portuguese","NL":"Dutch","SV":"Swedish","NO":"Norwegian","DA":"Danish",
    "FI":"Finnish","RU":"Russian","PL":"Polish","TR":"Turkish","ZH":"Chinese",
    "JA":"Japanese","KO":"Korean","AR":"Arabic","HE":"Hebrew","HI":"Hindi"
}

def normalize_language(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    if len(s) <= 3:
        return ISO_LANG.get(s.upper(), s.upper())
    return s


def _normalize_isbn(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s).replace("'", "") if ch.isdigit())


def append_record(tab: str, record: dict) -> None:
    """Ensure headers, dedupe (ISBN or Title+Author), preserve ISBN as text, then append."""
    try:
        ws = _get_ws(tab)
        if not ws:
            raise RuntimeError("Worksheet not found")
        # 1) Ensure headers in a fixed order (keep any extras at the end)
        headers = [h.strip() for h in ws.row_values(1)]
        if not headers:
            headers = EXACT_HEADERS[:]
            ws.update('A1', [headers])
        else:
            extras = [h for h in headers if h not in EXACT_HEADERS]
            headers = EXACT_HEADERS[:] + extras
            ws.update('A1', [headers])
        # 2) De-dup in this tab
        values = ws.get_all_values()
        existing_isbns, existing_ta = set(), set()
        i_isbn = headers.index("ISBN") if "ISBN" in headers else None
        i_title = headers.index("Title") if "Title" in headers else None
        i_author = headers.index("Author") if "Author" in headers else None
        for r in values[1:]:
            if i_isbn is not None and len(r) > i_isbn:
                norm = _normalize_isbn(r[i_isbn])
                if norm:
                    existing_isbns.add(norm)
            if i_title is not None and i_author is not None and len(r) > max(i_title, i_author):
                t = (r[i_title] or "").strip().lower()
                a = (r[i_author] or "").strip().lower()
                if t or a:
                    existing_ta.add((t, a))
        inc_isbn_norm = _normalize_isbn(record.get("ISBN", ""))
        inc_ta = ((record.get("Title", "").strip().lower()), (record.get("Author", "").strip().lower()))
        if inc_isbn_norm and inc_isbn_norm in existing_isbns:
            st.info(f"'{record.get('Title','(unknown)')}' is already in {tab} (same ISBN). Skipped.")
            return
        if inc_ta in existing_ta:
            st.info(f"'{record.get('Title','(unknown)')}' by {record.get('Author','?')} is already in {tab}. Skipped.")
            return
        # 3) Preserve ISBN as text, build row in header order, append
        if record.get("ISBN") and str(record["ISBN"]).isdigit():
            record["ISBN"] = "'" + str(record["ISBN"]).strip()
        keymap = {h.lower(): h for h in headers}
        row = [record.get(keymap.get(h.lower(), h), record.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="RAW")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")
        raise

# ---------- Metadata fetchers ----------
@st.cache_data(ttl=86400)
def get_book_details_google(isbn: str) -> dict:
    if not isbn:
        return {}
    try:
        params = {"q": f"isbn:{isbn}", "printType": "books", "maxResults": 1}
        if GOOGLE_BOOKS_KEY:
            params["key"] = GOOGLE_BOOKS_KEY
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params=params,
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            return {}
        info = items[0].get("volumeInfo", {})
        desc = info.get("description") or items[0].get("searchInfo", {}).get("textSnippet")
        thumbs = info.get("imageLinks") or {}
        thumb = thumbs.get("thumbnail") or thumbs.get("smallThumbnail") or ""
        if thumb.startswith("http://"):
            thumb = thumb.replace("http://", "https://")
        cats = info.get("categories") or []
        return {
            "ISBN": isbn,
            "Title": info.get("title", ""),
            "Author": ", ".join(info.get("authors", [])),
            "Genre": ", ".join(cats) if cats else "",
            "Language": (info.get("language") or "").upper(),
            "Thumbnail": thumb,
            "Description": (desc or "").strip(),
            "Rating": str(info.get("averageRating", "")),
            # Added publishedDate to the returned dictionary
            "PublishedDate": info.get("publishedDate", ""),
        }
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def _ol_fetch_json(url: str) -> dict:
    try:
        r = requests.get(url, timeout=12, headers={"User-Agent": "misiddons/1.0"})
        if r.ok:
            return r.json()
    except Exception:
        pass
    return {}

@st.cache_data(ttl=86400)
def get_openlibrary_rating(isbn: str):
    """Return (avg, count) rating for the book's first work on Open Library, if any."""
    try:
        bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json")
        works = bj.get("works") or []
        if not works:
            return None, None
        work_key = works[0].get("key")
        if not work_key:
            return None, None
        rj = _ol_fetch_json(f"https://openlibrary.org{work_key}/ratings.json")
        summary = rj.get("summary", {}) if isinstance(rj, dict) else {}
        avg = summary.get("average")
        count = summary.get("count")
        return (avg, count)
    except Exception:
        return None, None

@st.cache_data(ttl=86400)
def get_book_details_openlibrary(isbn: str) -> dict:
    """Robust OpenLibrary metadata with description & cover fallbacks via works/details endpoints."""
    try:
        r = requests.get(
            "https://openlibrary.org/api/books",
            params={"bibkeys": f"ISBN:{isbn}", "jscmd": "data", "format": "json"},
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}") or {}
        authors = ", ".join([a.get("name", "") for a in data.get("authors", []) if a])
        subjects = ", ".join([s.get("name", "") for s in data.get("subjects", []) if s])
        cover = (data.get("cover") or {}).get("large") or (data.get("cover") or {}).get("medium") or ""
        desc = data.get("description", "")
        if isinstance(desc, dict):
            desc = desc.get("value", "")
        # Extra fallback: jscmd=details may contain description
        if not desc:
            try:
                rd = requests.get(
                    "https://openlibrary.org/api/books",
                    params={"bibkeys": f"ISBN:{isbn}", "jscmd": "details", "format": "json"},
                    timeout=12,
                    headers={"User-Agent": "misiddons/1.0"},
                )
                if rd.ok:
                    details = (rd.json().get(f"ISBN:{isbn}") or {}).get("details", {})
                    d2 = details.get("description")
                    if isinstance(d2, dict):
                        d2 = d2.get("value")
                    if isinstance(d2, str) and d2:
                        desc = d2
            except Exception:
                pass
        # Fallback: /isbn and then /works
        if not desc or not cover or not data.get("languages"):
            bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json")
            if (not cover) and bj.get("covers"):
                try:
                    cover_id = bj["covers"][0]
                    cover = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
                except Exception:
                    pass
            works = bj.get("works") or []
            if works:
                wk = works[0].get("key")
                if wk:
                    wj = _ol_fetch_json(f"https://openlibrary.org{wk}.json")
                    d = wj.get("description", "")
                    if isinstance(d, dict):
                        d = d.get("value", "")
                    if d and not desc:
                        desc = d
        # Language
        lang = ""
        try:
            lang = (data.get("languages", [{}])[0].get("key", "").split("/")[-1] or "").upper()
        except Exception:
            bj = _ol_fetch_json(f"https://openlibrary.org/isbn/{isbn}.json")
            try:
                lang_codes = bj.get("languages", [])
                if lang_codes:
                    lang = lang_codes[0].get("key", "").split("/")[-1].upper()
            except Exception:
                lang = ""
        # Added publish_date to the returned dictionary
        published_date = data.get("publish_date", "")
        return {
            "ISBN": isbn,
            "Title": data.get("title", ""),
            "Author": authors,
            "Genre": subjects,
            "Language": lang,
            "Thumbnail": cover,
            "Description": (desc or "").strip(),
            "PublishedDate": published_date,
        }
    except Exception:
        return {}


def get_book_metadata(isbn: str) -> dict:
    """Merge Google + OpenLibrary so missing fields are filled, and compute ratings."""
    g = get_book_details_google(isbn)
    # Added "PublishedDate" to the list of keys to check
    need_keys = ["Description", "Thumbnail", "Language", "Genre", "Title", "Author", "PublishedDate"]
    o = {}
    if not g or any(not g.get(k) for k in need_keys):
        o = get_book_details_openlibrary(isbn)
    if not g and not o:
        return {}
    merged = {**o, **g}  # prefer Google when present
    for k in need_keys:
        if not merged.get(k) and (o.get(k)):
            merged[k] = o[k]
    merged["Language"] = normalize_language(merged.get("Language", "")) or ("English" if "english literature" in merged.get("Genre", "").lower() else "")
    # Ratings: Google + OpenLibrary (works)
    parts = []
    if g.get("Rating"):
        parts.append(f"GB:{g['Rating']}")
    ol_avg, ol_count = get_openlibrary_rating(isbn)
    if ol_avg:
        try:
            parts.append(f"OL:{round(float(ol_avg), 2)}")
        except Exception:
            parts.append(f"OL:{ol_avg}")
    merged["Rating"] = " | ".join(parts)
    # Added "PublishedDate" to the setdefault call
    for k in ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
        merged.setdefault(k, "")
    return merged

@st.cache_data(ttl=86400)
def get_recommendations_by_author(author: str) -> list:
    if not author:
        return []
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"inauthor:{quote(author)}", "maxResults": 8},
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        if r.ok:
            return r.json().get("items", [])
    except Exception:
        pass
    return []

# ---------- Barcode helpers ----------
def _extract_isbn_from_raw(raw: str) -> str:
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 13:
        return digits[-13:]
    return digits

# ---------- UI ----------
st.title("Misiddons Book Database")

# — Add Book Form —
with st.expander("✍️ Add a New Book", expanded=False):
    with st.form("entry_form"):
        # Pre-populate form fields with scanned data if available
        cols = st.columns(5)
        title = cols[0].text_input("Title", value=st.session_state.get("scan_title", ""))
        author = cols[1].text_input("Author", value=st.session_state.get("scan_author", ""))
        isbn = cols[2].text_input("ISBN (Optional)", value=st.session_state.get("scan_isbn", ""))
        date_read = cols[3].text_input("Date Read", placeholder="YYYY/MM/DD")
        choice = cols[4].radio("Add to:", ["Library", "Wishlist"], horizontal=True)

        if st.form_submit_button("Add Book"):
            if title and author:
                try:
                    scan_meta = st.session_state.get("last_scan_meta", {})
                    rec = {"ISBN": isbn, "Title": title, "Author": author, "Date Read": date_read}
                    # Add all other metadata from the scan to the record
                    for k in ["Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
                        if k in scan_meta and scan_meta[k] and k not in rec:
                            rec[k] = scan_meta[k]
                    append_record(choice, rec)
                    st.success(f"Added '{title}' to {choice}.")
                    # Clear session state for next entry
                    st.session_state["scan_isbn"] = ""
                    st.session_state["scan_title"] = ""
                    st.session_state["scan_author"] = ""
                    st.session_state["last_scan_meta"] = {}
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Failed to add book: {e}")
            else:
                st.warning("Enter both title and author.")

# — Barcode scanner (from image) —
if zbar_decode:
    with st.expander("📷 Scan Barcode from Photo"):
        up = st.file_uploader("Upload a photo of the barcode", type=["png","jpg","jpeg"])
        if up:
            try:
                img = Image.open(up)
                if img.mode != "RGB":
                    img = img.convert("RGB")
                codes = zbar_decode(img)
            except Exception:
                codes = []
            if not codes:
                st.warning("No barcode found. Try a closer, sharper photo.")
            else:
                raw = codes[0].data.decode(errors="ignore")
                isbn_bc = _extract_isbn_from_raw(raw)
                st.info(f"Detected code: {raw} → Using ISBN: {isbn_bc}")

                with st.spinner("Fetching book details..."):
                    meta = get_book_metadata(isbn_bc)

                if not meta or not meta.get("Title"):
                    st.error("Couldn't fetch details from Google/OpenLibrary. Check the ISBN or try again.")
                else:
                    # Update session state for the manual entry form
                    st.session_state["scan_isbn"] = meta.get("ISBN", "")
                    st.session_state["scan_title"] = meta.get("Title", "")
                    st.session_state["scan_author"] = meta.get("Author", "")
                    st.session_state["last_scan_meta"] = meta

                    with st.expander("Show raw metadata", expanded=False):
                        st.json(meta)
                    
                    cols = st.columns([1,3])
                    with cols[0]:
                        if meta.get("Thumbnail"):
                            st.image(meta["Thumbnail"], caption=meta.get("Title",""), width=150)
                    with cols[1]:
                        st.subheader(meta.get("Title","Unknown Title"))
                        st.write(f"**Author:** {meta.get('Author','Unknown')}")
                        st.write(f"**Published Date:** {meta.get('PublishedDate','Unknown')}")
                        if meta.get("Rating"):
                             st.write(f"**Rating:** {meta.get('Rating')}")
                        if meta.get("Language"):
                            st.write(f"**Language:** {meta.get('Language')}")

                    # Description with 'Read more' logic
                    full_desc = meta.get("Description", "")
                    if full_desc:
                        # Split the description by newlines to count lines
                        lines = full_desc.split('\n')
                        # Heuristic: show the first 5 lines or the first 500 characters
                        if len(lines) > 5 or len(full_desc) > 500:
                            short_desc = "\n".join(lines[:5])
                            with st.expander("Description (click to expand)"):
                                st.write(full_desc)
                        else:
                            st.caption(full_desc)

                    a1, a2 = st.columns(2)
                    with a1:
                        if st.button("➕ Add to Library", key="add_scan_lib", use_container_width=True):
                            try:
                                append_record("Library", meta)
                                st.success("Added to Library ✔")
                                st.session_state["scan_isbn"] = ""
                                st.session_state["scan_title"] = ""
                                st.session_state["scan_author"] = ""
                                st.session_state["last_scan_meta"] = {}
                                st.experimental_rerun()
                            except Exception:
                                pass
                    with a2:
                        if st.button("🧾 Add to Wishlist", key="add_scan_wl", use_container_width=True):
                            try:
                                append_record("Wishlist", meta)
                                st.success("Added to Wishlist ✔")
                                st.session_state["scan_isbn"] = ""
                                st.session_state["scan_title"] = ""
                                st.session_state["scan_author"] = ""
                                st.session_state["last_scan_meta"] = {}
                                st.experimental_rerun()
                            except Exception:
                                pass
else:
    st.info("Barcode scanning requires `pyzbar`/`zbar`. If unavailable in the environment, paste the ISBN manually above.")

st.divider()

# ---- Tabs ----
tabs = st.tabs(["Library", "Wishlist", "Recommendations"])
with tabs[0]:
    st.header("My Library")
    library_df = load_data("Library")
    if not library_df.empty:
        st.dataframe(library_df, use_container_width=True)
    else:
        st.info("Library is empty.")
with tabs[1]:
    st.header("My Wishlist")
    wishlist_df = load_data("Wishlist")
    if not wishlist_df.empty:
        st.dataframe(wishlist_df, use_container_width=True)
    else:
        st.info("Wishlist is empty.")
with tabs[2]:
    st.header("Recommendations")
    library_df = load_data("Library")
    if not library_df.empty and "Author" in library_df.columns:
        authors = library_df["Author"].dropna().unique()
        selected_author = st.selectbox("Find books by authors you've read:", authors)
        if selected_author:
            recommendations = get_recommendations_by_author(selected_author)
            if recommendations:
                for item in recommendations:
                    vi = item.get("volumeInfo", {})
                    cols = st.columns([1, 4])
                    with cols[0]:
                        thumb = vi.get("imageLinks", {}).get("thumbnail")
                        if thumb:
                            st.image(thumb, width=100)
                    with cols[1]:
                        st.subheader(vi.get("title", "No Title"))
                        st.write(f"**Author(s):** {', '.join(vi.get('authors', ['N/A']))}")
                        st.write(f"**Published:** {vi.get('publishedDate', 'N/A')}")
                        st.caption(vi.get("description", 'No description available.'))
                        st.markdown("---")
            else:
                st.info("No recommendations found.")
    else:
        st.info("Read some books to get recommendations!")


# ---- Diagnostics (safe to show) ----
with st.expander("Diagnostics – help me if it still fails"):
    try:
        acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
        st.write("Service account email:", acct)
        st.write("Spreadsheet ID in use:", SPREADSHEET_ID)
        try:
            test_client = connect_to_gsheets()
            if test_client:
                ss = test_client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else test_client.open(GOOGLE_SHEET_NAME)
                st.write("Found worksheet tabs:", [w.title for w in ss.worksheets()])
        except Exception as e:
            st.write("Open spreadsheet error:", f"{type(e).__name__}: {e}")
    except Exception as e:
        st.write("Diagnostics error:", f"{type(e).__name__}: {e}")
