# ---------- Sheet writer ----------
def update_cover_url_by_index(tab: str, df_index: int, new_url: str) -> bool:
    """Updates the 'Thumbnail' column for a specific row index."""
    try:
        ws = _get_ws(tab)
        if not ws:
            st.error("Could not connect to worksheet.")
            return False

        headers = ws.row_values(1)
        try:
            thumb_col_idx = headers.index("Thumbnail") + 1
        except ValueError:
            st.error("Your sheet is missing the 'Thumbnail' column.")
            return False

        sheet_row = df_index + 2
        ws.update_cell(sheet_row, thumb_col_idx, new_url)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Failed to update cover URL: {e}")
        return False

def append_record(tab: str, record: dict) -> None:
    """Ensure headers, dedupe (ISBN or Title+Author), preserve ISBN as text, then append."""
    try:
        ws = _get_ws(tab)
        if not ws:
            raise RuntimeError("Worksheet not found")

        headers = [h.strip() for h in ws.row_values(1)]
        if not headers:
            headers = EXACT_HEADERS[:]
            ws.update('A1', [headers])
        else:
            extras = [h for h in headers if h not in EXACT_HEADERS]
            headers = EXACT_HEADERS[:] + extras
            ws.update('A1', [headers])

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

        if record.get("ISBN") and str(record["ISBN"]).isdigit():
            record["ISBN"] = "'" + str(record["ISBN"]).strip()

        keymap = {h.lower(): h for h in headers}
        row = [record.get(keymap.get(h.lower(), h), record.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()

    except Exception as e:
        st.error(f"Failed to write to '{tab}': {e}")
        raise

# ---------- UI ----------

st.title("Misiddons Book Database")

# Initialize session state for form and scanner if not present
for k, v in {
    "scan_isbn": "",
    "scan_title": "",
    "scan_author": "",
    "last_scan_meta": {},
}.items():
    st.session_state.setdefault(k, v)

# --- Add Book Form ---
with st.expander("✍️ Add a New Book Manually", expanded=False):
    with st.form("entry_form"):
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
                    for k in ["Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
                        if k in scan_meta and scan_meta[k]:
                            rec[k] = scan_meta[k]

                    # Normalized de-dupe across both tabs
                    lib_df = load_data("Library")
                    wish_df = load_data("Wishlist")

                    for df in (lib_df, wish_df):
                        if not df.empty:
                            for col in ["ISBN","Title","Author"]:
                                if col not in df.columns:
                                    df[col] = ""

                    all_df = pd.concat([lib_df, wish_df], ignore_index=True) if not lib_df.empty or not wish_df.empty else pd.DataFrame(columns=["ISBN","Title","Author"])

                    existing_isbns = set(all_df["ISBN"].astype(str).map(_normalize_isbn).dropna()) if not all_df.empty else set()
                    existing_ta = set(zip(
                        all_df.get("Title", pd.Series(dtype=str)).fillna("").str.strip().str.lower(),
                        all_df.get("Author", pd.Series(dtype=str)).fillna("").str.strip().str.lower(),
                    )) if not all_df.empty else set()

                    inc_isbn_norm = _normalize_isbn(rec.get("ISBN",""))
                    inc_ta = (rec.get("Title","").strip().lower(), rec.get("Author","").strip().lower())

                    if inc_isbn_norm and inc_isbn_norm in existing_isbns:
                        st.warning(f"This book (ISBN: {rec.get('ISBN','')}) already exists in Library/Wishlist. Skipped.")
                    elif inc_ta in existing_ta:
                        st.warning(f"'{rec['Title']}' by {rec['Author']} already exists in Library/Wishlist. Skipped.")
                    else:
                        append_record(choice, rec)
                        st.success(f"Added '{title}' to {choice} 🎉")
                        for k in ("scan_isbn","scan_title","scan_author"):
                            st.session_state[k] = ""
                        st.session_state["last_scan_meta"] = {}
                        st.rerun()
                except Exception as e:
                    st.error(f"Failed to add book: {e}")
            else:
                st.warning("Enter both a title and author to add a book.")

# --- Barcode scanner (from image) ---
if zbar_decode:
    with st.expander("📷 Scan Barcode from Photo", expanded=False):
        up = st.file_uploader("Upload a clear photo of the barcode", type=["png", "jpg", "jpeg"])
        if up:
            try:
                img = Image.open(up)
                if img.mode != "RGB":
                    img = img.convert("RGB")
                codes = zbar_decode(img)
            except Exception:
                codes = []

            if not codes:
                st.warning("No barcode found. Please try a closer, sharper photo.")
            else:
                raw = codes[0].data.decode(errors="ignore")
                digits = "".join(ch for ch in raw if ch.isdigit())
                isbn_bc = digits[-13:] if len(digits) >= 13 else digits
                st.info(f"Detected code: {raw} → Using ISBN: {isbn_bc}")

                with st.spinner("Fetching book details..."):
                    @st.cache_data(ttl=86400)
def get_book_metadata(isbn: str) -> dict:
    google_meta = get_book_details_google(isbn)
    openlibrary_meta = get_book_details_openlibrary(isbn)

    # Prefer Google if it found a title; otherwise use OpenLibrary
    meta = google_meta.copy() if google_meta.get("Title") else openlibrary_meta.copy()

    # Backfill from the other source
    for key in ["Title", "Author", "Genre", "Language", "Thumbnail", "Description", "PublishedDate"]:
        if not meta.get(key):
            meta[key] = openlibrary_meta.get(key, "") if meta is google_meta else google_meta.get(key, "")

    # Ensure keys exist
    for k in ["ISBN","Title","Author","Genre","Language","Thumbnail","Description","Rating","PublishedDate"]:
        meta.setdefault(k, "")

    # Language prettifier (not shown in grid, but kept elsewhere)
    meta["Language"] = _pretty_lang(meta.get("Language", ""))

    # Cover fallback
    isbn = (meta.get("ISBN") or isbn or "").strip()
    if not meta.get("Thumbnail") and isbn:
        meta["Thumbnail"] = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"

    # ⭐ Ratings: match your sample — ONLY OL + GR placeholder (no Google rating badge)
    ratings_parts = []
    ol_avg, _ = get_openlibrary_rating(isbn)
    if ol_avg is not None:
        try:
            # keep natural string (3.8, 4.0, 3.55, etc.)
            ol_val = round(float(ol_avg), 2)
            ratings_parts.append(f"OL:{ol_val}")
        except Exception:
            ratings_parts.append(f"OL:{ol_avg}")
    ratings_parts.append("GR:unavailable")
    # If OL is missing, you'll just see "GR:unavailable"
    meta["Rating"] = " | ".join([p for p in ratings_parts if p])

    # Known name fix + primary author
    if meta.get("Author") == "Jø Lier Horst":
        meta["Author"] = "Jørn Lier Horst"
    meta["Author"] = keep_primary_author(meta.get("Author", ""))

    return meta


# --- Tabs ---
tabs = st.tabs(["Library", "Wishlist", "Statistics", "Recommendations"])

with tabs[0]:
    st.header("My Library")
    library_df = load_data("Library")

    if library_df.empty:
        st.info("Your library is empty. Add a book to get started!")
    else:
        # Search
        search_lib = st.text_input(
            "🔎 Search My Library...",
            placeholder="Search titles, authors, or genres...",
            key="lib_search"
        )

        lib_df_display = library_df.copy()
        if search_lib:
            lib_df_display = lib_df_display[
                lib_df_display.apply(
                    lambda row: row.astype(str).str.contains(search_lib, case=False, na=False).any(),
                    axis=1
                )
            ]

        # Ensure columns for rendering exist
        for col in ["Thumbnail", "Title", "Author"]:
            if col not in lib_df_display.columns:
                lib_df_display[col] = ""

        # Render the polished card grid (rating-only badges)
        render_library_grid(lib_df_display)

        # Focused panel to fix missing covers
        missing = lib_df_display[lib_df_display.get("Thumbnail","").astype(str).str.strip() == ""]
        if not missing.empty:
            with st.expander("🖼️ Add missing covers", expanded=False):
                st.caption("Paste a valid https:// image URL for any book without a cover.")
                for idx, row in missing.iterrows():
                    cols = st.columns([3,2])
                    with cols[0]:
                        st.markdown(f"**{row.get('Title','Untitled')}**")
                        st.caption(row.get("Author","Unknown"))
                    with cols[1]:
                        with st.form(key=f"fix_cover_{idx}"):
                            url = st.text_input("Image URL", key=f"url_{idx}", label_visibility="collapsed",
                                                placeholder="https://…")
                            if st.form_submit_button("Save"):
                                if url.startswith("http"):
                                    if update_cover_url_by_index("Library", int(idx), url):
                                        st.success("Saved. Refreshing…")
                                        st.rerun()
                                else:
                                    st.warning("Please enter a valid URL (starting with http).")

with tabs[1]:
    st.header("My Wishlist")
    wishlist_df = load_data("Wishlist")
    if not wishlist_df.empty:
        search_wish = st.text_input("🔎 Search My Wishlist...", placeholder="Search titles, authors, or genres...", key="wish_search")

        wish_df_display = wishlist_df.copy()
        if search_wish:
            wish_df_display = wish_df_display[
                wish_df_display.apply(lambda row: row.astype(str).str.contains(search_wish, case=False, na=False).any(), axis=1)
            ]

        st.dataframe(
            wish_df_display,
            use_container_width=True,
            column_config={
                "Thumbnail": st.column_config.ImageColumn("Cover", width="small"),
                "Description": st.column_config.TextColumn("Description", help="Summary of the book", width="large")
            },
            hide_index=True
        )
    else:
        st.info("Your wishlist is empty. Scan a book or add one manually!")

with tabs[2]:
    st.header("Statistics")
    library_df = load_data("Library")
    wishlist_df = load_data("Wishlist")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Books in Library", len(library_df))
    with col2:
        st.metric("Total Books on Wishlist", len(wishlist_df))
    with col3:
        uniq_auth = 0 if library_df.empty or "Author" not in library_df.columns else library_df["Author"].fillna("").astype(str).str.split(",").explode().str.strip().replace({"": None}).dropna().nunique()
        st.metric("Unique Authors (Library)", int(uniq_auth))

with tabs[3]:
    st.header("Recommendations")
    library_df = load_data("Library")
    wishlist_df = load_data("Wishlist")

    # Collect owned titles/ISBNs to filter out
    owned_titles = set()
    owned_isbns = set()
    for df in (library_df, wishlist_df):
        if not df.empty:
            if "Title" in df.columns:
                owned_titles.update(df["Title"].dropna().astype(str).str.lower().str.strip().tolist())
            if "ISBN" in df.columns:
                owned_isbns.update(df["ISBN"].dropna().astype(str).map(_normalize_isbn).tolist())

    # Build author list from Library
    authors = []
    if not library_df.empty and "Author" in library_df.columns:
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

    if mode == "By author":
        if authors:
            selected_author = st.selectbox("Find books by authors you've read:", authors)
        else:
            selected_author = st.text_input("Type an author to get recommendations:")

        if selected_author:
            recommendations = get_recommendations_by_author(selected_author)

            shown = 0
            for item in recommendations:
                title = (item.get("title") or "").strip()
                isbn = _normalize_isbn(item.get("isbn", ""))
                if (title.lower() in owned_titles) or (isbn and isbn in owned_isbns):
                    continue

                cols = st.columns([1, 4])
                with cols[0]:
                    thumb, _ = _cover_or_placeholder(item.get("thumbnail", ""), title)
                    st.image(thumb, width=100)
                with cols[1]:
                    st.subheader(title or "No Title")
                    st.write(f"**Author(s):** {item.get('authors', 'N/A')}")
                    st.write(f"**Published:** {item.get('published', 'N/A')}")
                    if item.get("description"):
                        st.caption(item["description"]) 

                    add_key = f"rec_add_{selected_author}_{shown}"
                    if st.button("🧾 Add to Wishlist", key=add_key):
                        rec_meta = {
                            "ISBN": isbn,
                            "Title": title,
                            "Author": item.get("authors", ""),
                            "Genre": "",
                            "Language": "",
                            "Thumbnail": item.get("thumbnail", ""),
                            "Description": (item.get("description") or ""),
                            "Rating": "",
                            "PublishedDate": item.get("published", ""),
                        }
                        try:
                            append_record("Wishlist", rec_meta)
                            st.success(f"Added '{title}' to Wishlist")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not add: {e}")
                    st.markdown("---")
                shown += 1
                if shown >= 5:
                    break

            if shown == 0:
                st.info("No new recommendations found (everything shown is already in your Library/Wishlist or nothing was returned by the sources). Try another author.")

    else:  # Surprise me (4 random unseen)
        if not authors:
            st.info("Add at least one book with an author to your Library to get surprise recommendations.")
        else:
            sample_authors = random.sample(authors, k=min(6, len(authors)))
            pool: list[dict] = []
            for a in sample_authors:
                pool.extend(get_recommendations_by_author(a))
            # Filter out owned and blanks
            filtered = []
            for item in pool:
                title = (item.get("title") or "").strip()
                isbn = _normalize_isbn(item.get("isbn", ""))
                if not title:
                    continue
                if (title.lower() in owned_titles) or (isbn and isbn in owned_isbns):
                    continue
                filtered.append(item)
            random.shuffle(filtered)
            picks = filtered[:4]

            if not picks:
                st.info("Couldn't find unseen picks right now. Try switching to 'By author' mode.")
            for idx, item in enumerate(picks, 1):
                title = (item.get("title") or "").strip()
                cols = st.columns([1, 4])
                with cols[0]:
                    thumb, _ = _cover_or_placeholder(item.get("thumbnail", ""), title)
                    st.image(thumb, width=100)
                with cols[1]:
                    st.subheader(f"{idx}. {title or 'No Title'}")
                    st.write(f"**Author(s):** {item.get('authors', 'N/A')}")
                    st.write(f"**Published:** {item.get('published', 'N/A')}")
                    if item.get("description"):
                        st.caption(item["description"]) 

                    isbn = _normalize_isbn(item.get("isbn", ""))
                    add_key = f"rec_surprise_add_{idx}"
                    if st.button("🧾 Add to Wishlist", key=add_key):
                        rec_meta = {
                            "ISBN": isbn,
                            "Title": title,
                            "Author": item.get("authors", ""),
                            "Genre": "",
                            "Language": "",
                            "Thumbnail": item.get("thumbnail", ""),
                            "Description": (item.get("description") or ""),
                            "Rating": "",
                            "PublishedDate": item.get("published", ""),
                        }
                        try:
                            append_record("Wishlist", rec_meta)
                            st.success(f"Added '{title}' to Wishlist")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not add: {e}")
                st.markdown("---")

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

# ==== Data Check (Library) =====================================================
with st.expander("🔍 Data Check — Library", expanded=False):
    lib = load_data("Library")

    if lib.empty:
        st.info("Library sheet is empty.")
    else:
        for c in ["ISBN", "Title", "Author", "Language", "Thumbnail", "PublishedDate", "Date Read", "Description"]:
            if c not in lib.columns:
                lib[c] = ""

        lib["_isbn_norm"]   = lib["ISBN"].astype(str).map(_normalize_isbn)
        lib["_author_primary"] = lib["Author"].astype(str).map(keep_primary_author)
        lib["_title_norm"]  = lib["Title"].astype(str).str.strip().str.lower()
        lib["_ta_key"]      = lib["_title_norm"] + " | " + lib["_author_primary"].str.strip().str.lower()

        issues = []

        # 1) Title/Author missing
        mask_missing = (lib["Title"].astype(str).str.strip() == "") | (lib["Author"].astype(str).str.strip() == "")
        for i, r in lib[mask_missing].iterrows():
            issues.append({
                "Row": i+2,
                "Issue": "Missing Title or Author",
                "Title": r["Title"], "Author": r["Author"], "ISBN": r["ISBN"],
                "Suggestion": "Fill in missing field(s)."
            })

        # 2) Author not reduced to primary
        mask_author_multi = lib["Author"].astype(str) != lib["_author_primary"]
        for i, r in lib[mask_author_multi].iterrows():
            issues.append({
                "Row": i+2,
                "Issue": "Author list not normalized",
                "Title": r["Title"], "Author": r["Author"], "ISBN": r["ISBN"],
                "Suggestion": f"Use primary author → '{r['_author_primary']}'."
            })

        # 3) Duplicate ISBNs (non-empty)
        dup_isbn = lib[lib["_isbn_norm"] != ""]
        dup_isbn = dup_isbn[dup_isbn["_isbn_norm"].duplicated(keep=False)].sort_values("_isbn_norm")
        for i, r in dup_isbn.iterrows():
            issues.append({
                "Row": i+2,
                "Issue": "Duplicate ISBN",
                "Title": r["Title"], "Author": r["_author_primary"], "ISBN": r["ISBN"],
                "Suggestion": "Remove duplicate or correct ISBN."
            })

        # 4) Duplicate Title+Author (case-insensitive)
        dup_ta = lib[lib["_ta_key"].duplicated(keep=False)].sort_values("_ta_key")
        for i, r in dup_ta.iterrows():
            issues.append({
                "Row": i+2,
                "Issue": "Duplicate Title+Author",
                "Title": r["Title"], "Author": r["_author_primary"], "ISBN": r["ISBN"],
                "Suggestion": "Remove duplicate row."
            })

        # 5) Non-HTTPS cover URLs
        bad_thumb = lib["Thumbnail"].astype(str).str.startswith("http://", na=False)
        for i, r in lib[bad_thumb].iterrows():
            issues.append({
                "Row": i+2,
                "Issue": "Insecure cover URL (http)",
                "Title": r["Title"], "Author": r["_author_primary"], "ISBN": r["ISBN"],
                "Suggestion": "Switch to https:// thumbnail."
            })

        # 6) Date Read format check
        date_mask = lib["Date Read"].astype(str).str.strip() != ""
        bad_date = ~lib.loc[date_mask, "Date Read"].astype(str).str.match(r"^\d{4}/\d{2}/\d{2}$", na=False)
        for i, r in lib.loc[date_mask].loc[bad_date].iterrows():
            issues.append({
                "Row": i+2,
                "Issue": "Date Read format",
                "Title": r["Title"], "Author": r["_author_primary"], "ISBN": r["ISBN"],
                "Suggestion": "Use YYYY/MM/DD."
            })

        st.metric("Rows in Library", len(lib))
        st.metric("Unique ISBNs", int((lib["_isbn_norm"] != "").sum() - lib.loc[lib["_isbn_norm"] != "", "_isbn_norm"].duplicated().sum()))
        st.metric("Unique Title+Author", int(lib["_ta_key"].nunique()))

        if issues:
            prob_df = pd.DataFrame(issues, columns=["Row","Issue","Title","Author","ISBN","Suggestion"])
            st.warning(f"Found {len(prob_df)} potential issue(s).")
            st.dataframe(prob_df, use_container_width=True, hide_index=True)
        else:
            st.success("Looks good! No issues detected in Library 🎉")


# ==== Cross-check Authors & Titles (Library) ===================================
with st.expander("🔎 Cross-check — Authors & Titles (Library)", expanded=False):
    lib = load_data("Library")
    if lib.empty:
        st.info("Library sheet is empty.")
    else:
        for c in ["ISBN", "Title", "Author"]:
            if c not in lib.columns:
                lib[c] = ""

        rows = []
        issues = []
        for i, r in lib.iterrows():
            sheet_title  = str(r["Title"]).strip()
            sheet_author = str(r["Author"]).strip()
            sheet_isbn   = str(r["ISBN"]).strip()

            if not sheet_title and not sheet_author:
                continue

            can = _canonical_from_row(sheet_title, sheet_author, sheet_isbn)
            if not can:
                rows.append({
                    "Row": i+2, "ISBN": sheet_isbn,
                    "Sheet Title": sheet_title, "Sheet Author": sheet_author,
                    "Canonical Title": "(not found)", "Canonical Author": "(not found)",
                    "Title Match": "n/a", "Author Match": "n/a", "Source": "n/a", "Note": "No external match"
                })
                continue

            nt_s = _norm_title(sheet_title);  nt_c = _norm_title(can["Title"])
            na_s = _norm_author(sheet_author); na_c = _norm_author(can["Author"])

            t_ratio = SequenceMatcher(None, nt_s, nt_c).ratio() if nt_c else 0.0
            a_ratio = SequenceMatcher(None, na_s, na_c).ratio() if na_c else 0.0

            t_match = "exact" if nt_s == nt_c else ("close" if t_ratio >= 0.85 else "diff")
            a_match = "exact" if na_s == na_c else ("close" if a_ratio >= 0.85 else "diff")

            note = ""
            if t_match == "diff":
                note += "Title differs. "
            if a_match == "diff":
                note += "Author differs. "
            if not note and (t_match == "close" or a_match == "close"):
                note = "Minor variance (edition/subtitle/diacritics)."

            row_info = {
                "Row": i+2, "ISBN": sheet_isbn,
                "Sheet Title": sheet_title, "Canonical Title": can["Title"], "Title Match": t_match,
                "Sheet Author": sheet_author, "Canonical Author": can["Author"], "Author Match": a_match,
                "Source": can.get("source",""), "Note": note.strip()
            }
            rows.append(row_info)
            if t_match != "exact" or a_match != "exact":
                issues.append(row_info)

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        if issues:
            st.warning(f"{len(issues)} row(s) need attention. Look at 'diff' rows and update the sheet if needed.")
        else:
            st.success("All titles & authors match the external sources 🎯")
