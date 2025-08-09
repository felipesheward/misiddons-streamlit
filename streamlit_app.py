#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Misiddons Book Database – Streamlit app (Scan-only mode)
- Uses device camera to scan barcodes
- Prefers back camera on mobile (Streamlit's camera widget usually selects it)
- Fetches metadata (title, author, cover, description)
- Lets you add directly to Library or Wishlist
"""

from __future__ import annotations
import io
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
# Secrets should contain your service account + (optionally) your sheet id/name
# Fallbacks provided in case secrets are missing
DEFAULT_SHEET_ID   = "1AXupO4-kABwoz88H2dYfc6hv6wzooh7f8cDnIRl0Q7s"
SPREADSHEET_ID     = st.secrets.get("google_sheet_id", DEFAULT_SHEET_ID)
GOOGLE_SHEET_NAME  = st.secrets.get("google_sheet_name", "database")

st.set_page_config(page_title="Misiddons Book Database – Scanner", layout="wide")

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
def _get_ws(tab: str):
    client = connect_to_gsheets()
    if not client:
        return None
    ss = client.open_by_key(SPREADSHEET_ID) if SPREADSHEET_ID else client.open(GOOGLE_SHEET_NAME)
    # Try exact then forgiving match
    t = tab.strip()
    try:
        return ss.worksheet(t)
    except WorksheetNotFound:
        names = [w.title for w in ss.worksheets()]
        norm = {n.strip().casefold(): n for n in names}
        if t.casefold() in norm:
            return ss.worksheet(norm[t.casefold()])
        raise

# Preserve ISBN as text and append using current header order
def append_record(tab: str, record: dict) -> None:
    try:
        ws = _get_ws(tab)
        if not ws:
            raise RuntimeError("Worksheet not found")
        headers = ws.row_values(1) or ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating"]
        # keep ISBN as text
        if record.get("ISBN") and str(record["ISBN"]).isdigit():
            record["ISBN"] = "'" + str(record["ISBN"]).strip()
        keymap = {h.lower(): h for h in headers}
        row = [record.get(keymap.get(h.lower(), h), record.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="RAW")
        st.cache_data.clear()  # refresh caches
    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")
        raise

# ---------- Metadata fetchers ----------
@st.cache_data(ttl=86400)
def get_book_details_google(isbn: str) -> dict:
    if not isbn:
        return {}
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"isbn:{isbn}", "printType": "books", "maxResults": 1},
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
        cats = info.get("categories") or []
        return {
            "ISBN": isbn,
            "Title": info.get("title", ""),
            "Author": ", ".join(info.get("authors", [])),
            "Genre": ", ".join(cats) if cats else "",
            "Language": (info.get("language") or "").upper(),
            "Thumbnail": thumb,
            "Description": desc or "",
        }
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def get_book_details_openlibrary(isbn: str) -> dict:
    try:
        r = requests.get(
            "https://openlibrary.org/api/books",
            params={"bibkeys": f"ISBN:{isbn}", "jscmd": "data", "format": "json"},
            timeout=12,
            headers={"User-Agent": "misiddons/1.0"},
        )
        r.raise_for_status()
        data = r.json().get(f"ISBN:{isbn}")
        if not data:
            return {}
        authors = ", ".join([a.get("name", "") for a in data.get("authors", []) if a])
        subjects = ", ".join([s.get("name", "") for s in data.get("subjects", []) if s])
        cover = (data.get("cover") or {}).get("medium") or (data.get("cover") or {}).get("large") or ""
        lang = ""
        if data.get("languages"):
            try:
                lang = data["languages"][0]["key"].split("/")[-1].upper()
            except Exception:
                lang = ""
        return {
            "ISBN": isbn,
            "Title": data.get("title", ""),
            "Author": authors,
            "Genre": subjects,
            "Language": lang,
            "Thumbnail": cover,
            "Description": data.get("notes", "") if isinstance(data.get("notes"), str) else "",
        }
    except Exception:
        return {}

def get_book_metadata(isbn: str) -> dict:
    meta = get_book_details_google(isbn)
    return meta or get_book_details_openlibrary(isbn)

# ---------- Barcode helpers ----------
def _extract_isbn_from_raw(raw: str) -> str:
    # Keep only digits, then prefer last 13 digits (EAN-13); fall back to 10 if present
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 13:
        return digits[-13:]
    return digits

def decode_isbn_from_image(img: Image.Image) -> str:
    if not zbar_decode:
        return ""
    try:
        # Ensure RGB for pyzbar
        if img.mode != "RGB":
            img = img.convert("RGB")
        codes = zbar_decode(img)
        if not codes:
            return ""
        raw = codes[0].data.decode(errors="ignore")
        return _extract_isbn_from_raw(raw)
    except Exception:
        return ""

# ---------- UI (Scan-only) ----------
st.title("📚 Misiddons – Scan & Add")

if not zbar_decode:
    st.error("Barcode scanning requires `pyzbar` + the system library `zbar`. Install them or use manual ISBN entry.")
else:
    st.caption("Tip: On phones, the camera widget usually opens the **back camera**. If not, flip it in the camera UI.")
    cam_img = st.camera_input("Point at the book's barcode and take a photo")

    if cam_img is not None:
        image = Image.open(cam_img)
        isbn = decode_isbn_from_image(image)
        if not isbn:
            st.warning("Couldn't detect a barcode. Try getting closer and ensure good lighting.")
        else:
            st.success(f"Detected ISBN: {isbn}")
            meta = get_book_metadata(isbn)
            if not meta:
                st.error("Couldn't fetch details from Google/OpenLibrary for this ISBN.")
            else:
                # Preview card
                cols = st.columns([1, 2])
                with cols[0]:
                    if meta.get("Thumbnail"):
                        st.image(meta["Thumbnail"], caption=meta.get("Title", ""))
                with cols[1]:
                    st.subheader(meta.get("Title", "Unknown Title"))
                    st.write(f"**Author:** {meta.get('Author','Unknown')}")
                    if meta.get("Genre"):
                        st.write(f"**Genre:** {meta.get('Genre')}")
                    if meta.get("Language"):
                        st.write(f"**Language:** {meta.get('Language')}")
                    if meta.get("Description"):
                        desc = meta["Description"]
                        st.caption(desc[:800] + ("…" if len(desc) > 800 else ""))

                # Actions
                a1, a2 = st.columns(2)
                with a1:
                    if st.button("➕ Add to Library", use_container_width=True):
                        try:
                            append_record("Library", meta)
                            st.success("Added to Library ✔")
                        except Exception:
                            pass
                with a2:
                    if st.button("🧾 Add to Wishlist", use_container_width=True):
                        try:
                            append_record("Wishlist", meta)
                            st.success("Added to Wishlist ✔")
                        except Exception:
                            pass

st.divider()

# ---- Diagnostics (optional) ----
with st.expander("Diagnostics"):
    acct = st.secrets.get("gcp_service_account", {}).get("client_email", "(missing)")
    st.write("Service account:", acct)
    st.write("Spreadsheet ID in use:", SPREADSHEET_ID)
    try:
        ws_titles = [w.title for w in (_get_ws("Library").spreadsheet.worksheets())]
        st.write("Tabs:", ws_titles)
    except Exception as e:
        st.write("Sheet access:", str(e))
