import json
import os
import re
import time
import html
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

BASE_URL = "https://api.company-information.service.gov.uk"
AUTO_REFRESH_SECONDS = 3
AUTO_RUN_EVERY_SECONDS = 60

TARGET_POSTCODE_PREFIXES = {
    "OX1", "OX2", "OX3", "OX4", "OX11", "OX14",
    "CB1", "CB2", "CB3", "CB4", "CB21", "CB22", "CB23", "CB24",
    "M1", "M2", "M13", "M14", "M15", "M50",
    "BT1", "BT2", "BT3", "BT4", "BT5", "BT7", "BT8", "BT9",
    "EC1V", "EC1", "E1", "N1", "N7", "W12", "E20", "E14", "SE1",
    "EH1", "EH2", "EH3", "EH8", "EH9", "EH12", "EH16",
    "B1", "B2", "B3", "B4", "B5", "B7", "B12", "B15", "B19",
    "BS1", "BS2", "BS3", "BS8", "BS9", "BS16", "BS34"
}

TECH_SIC_CODES = {
    "58210", "58290", "59111", "59113", "59120", "59140", "59133", "59200",
    "60100", "60200", "61100", "61200", "61300", "61900", "62011", "62012",
    "62020", "62030", "62090", "63110", "63120", "71121", "71122", "71200",
    "72110", "72190", "72200", "82290"
}

HOLDINGS_SIC_CODES = {
    "64201", "64202", "64203", "64204", "64205", "64209", "66300"
}

TARGET_SIC_CODES = TECH_SIC_CODES | HOLDINGS_SIC_CODES

BUZZWORD_TERMS = [
    "Bidco", "Holdco", "Topco", "Midco", "Labs", "UK", "EMEA",
    "Europe", "Pty", "PLC", "Pvt", "BV", "B.V", "Capital",
    "Investment", "Ventures"
]

TARGET_COUNTRIES = {
    "united states", "usa", "us",
    "germany", "france", "netherlands", "spain", "finland",
    "denmark", "norway", "sweden", "austria", "poland",
    "greece", "portugal", "italy", "belgium", "hong kong"
}

SEEN_FILE = "seen_companies.json"
OFFICER_CACHE_FILE = "officer_appointments_cache.json"
RESULTS_FILE = "companies_house_results.csv"

SIC_GROUP_MAP = {}
for code in TECH_SIC_CODES:
    SIC_GROUP_MAP[code] = "Tech"
for code in HOLDINGS_SIC_CODES:
    SIC_GROUP_MAP[code] = "Holdings"


def inject_auto_refresh(seconds: int):
    components.html(
        f"""
        <html>
            <head>
                <meta http-equiv="refresh" content="{seconds}">
            </head>
            <body></body>
        </html>
        """,
        height=0,
        width=0,
    )


def parse_key_string(raw: str) -> List[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def get_api_keys_from_sources() -> List[str]:
    try:
        if "COMPANIES_HOUSE_API_KEYS" in st.secrets:
            raw = st.secrets["COMPANIES_HOUSE_API_KEYS"]
            if isinstance(raw, str):
                return parse_key_string(raw)
            if isinstance(raw, list):
                return [str(x).strip() for x in raw if str(x).strip()]
    except Exception:
        pass

    env_value = os.getenv("COMPANIES_HOUSE_API_KEYS", "")
    if env_value:
        return parse_key_string(env_value)

    return []


class RotatingCHClient:
    def __init__(self, api_keys: List[str], rotate_every: int = 599):
        if not api_keys:
            raise ValueError("At least one Companies House API key is required.")
        self.api_keys = api_keys
        self.rotate_every = rotate_every
        self.key_index = 0
        self.request_count_on_key = 0
        self.session = requests.Session()

    def _rotate_key_if_needed(self):
        if self.request_count_on_key >= self.rotate_every:
            self.key_index = (self.key_index + 1) % len(self.api_keys)
            self.request_count_on_key = 0

    def _auth(self) -> Tuple[str, str]:
        return (self.api_keys[self.key_index], "")

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        retries = 0
        while retries < 5:
            self._rotate_key_if_needed()
            url = f"{BASE_URL}{path}"
            resp = self.session.get(url, params=params, auth=self._auth(), timeout=30)
            self.request_count_on_key += 1

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return {}
            if resp.status_code == 429:
                self.key_index = (self.key_index + 1) % len(self.api_keys)
                self.request_count_on_key = 0
                retries += 1
                time.sleep(2)
                continue
            if 500 <= resp.status_code < 600:
                retries += 1
                time.sleep(2)
                continue

            raise RuntimeError(f"Request failed: {resp.status_code} {resp.text[:500]}")

        raise RuntimeError(f"Failed after retries for path {path}")


def load_json_file(path: str, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def daterange_chunks(start_date: datetime, end_date: datetime, chunk_days: int = 7):
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def normalize_country(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def trim_postcode_area(postcode: Optional[str]) -> str:
    if not postcode:
        return ""
    postcode = postcode.strip().upper()
    return postcode[:-3].strip() if len(postcode) > 3 else postcode


def postcode_prefix_matches(postcode: Optional[str]) -> bool:
    if not postcode:
        return False
    postcode = postcode.strip().upper()
    return any(postcode.startswith(prefix) for prefix in TARGET_POSTCODE_PREFIXES)


def sic_matches(company_sic_codes: List[str]) -> bool:
    return any(code in TARGET_SIC_CODES for code in (company_sic_codes or []))


def name_has_buzzwords(company_name: str) -> bool:
    name = (company_name or "").lower()
    return any(term.lower() in name for term in BUZZWORD_TERMS)


def get_sic_group(company_sic_codes: List[str], company_name: str) -> str:
    groups = []
    for code in company_sic_codes or []:
        group = SIC_GROUP_MAP.get(code)
        if group and group not in groups:
            groups.append(group)
    if name_has_buzzwords(company_name) and "Buzzwords" not in groups:
        groups.append("Buzzwords")
    return ", ".join(groups) if groups else "Other"


def get_company_officers(client: RotatingCHClient, company_number: str) -> List[dict]:
    data = client.get(f"/company/{company_number}/officers")
    return data.get("items", [])


def is_active_director(officer: dict) -> bool:
    role = (officer.get("officer_role") or "").lower()
    resigned_on = officer.get("resigned_on")
    name = officer.get("name")
    return role == "director" and not resigned_on and bool(name)


def get_officer_id(officer: dict) -> Optional[str]:
    links = officer.get("links", {})
    officer_link = links.get("officer", {})
    appointments = officer_link.get("appointments")
    if appointments and "/officers/" in appointments:
        try:
            return appointments.split("/officers/")[1].split("/appointments")[0]
        except Exception:
            return None
    return None


def get_officer_appointments_count(client: RotatingCHClient, officer_id: str, cache: dict) -> int:
    if officer_id in cache:
        return cache[officer_id]
    data = client.get(f"/officers/{officer_id}/appointments")
    total_results = data.get("total_results")
    count = total_results if total_results is not None else len(data.get("items", []))
    cache[officer_id] = count
    return count


def advanced_search_companies(client: RotatingCHClient, params: dict) -> List[dict]:
    results = []
    start_index = 0
    size = 5000

    while True:
        page_params = dict(params)
        page_params["size"] = size
        page_params["start_index"] = start_index
        data = client.get("/advanced-search/companies", params=page_params)
        items = data.get("items", [])
        if not items:
            break

        results.extend(items)

        if len(items) < size:
            break

        start_index += size
        if start_index >= 10000:
            break

    return results


def search_sic_companies(client: RotatingCHClient, start_date: str, end_date: str) -> List[dict]:
    return advanced_search_companies(
        client,
        {
            "incorporated_from": start_date,
            "incorporated_to": end_date,
            "sic_codes": ",".join(sorted(TARGET_SIC_CODES)),
        },
    )


def search_buzzword_companies(client: RotatingCHClient, start_date: str, end_date: str) -> List[dict]:
    results = []
    seen_numbers = set()
    for term in BUZZWORD_TERMS:
        items = advanced_search_companies(
            client,
            {
                "incorporated_from": start_date,
                "incorporated_to": end_date,
                "company_name_includes": term,
            },
        )
        for item in items:
            company_number = item.get("company_number")
            if company_number and company_number not in seen_numbers:
                seen_numbers.add(company_number)
                results.append(item)
    return results


def make_assumed_email(first_director_name: str, company_name: str) -> str:
    if not first_director_name or not company_name:
        return ""

    first_name = first_director_name.strip().split()[0].lower()
    company_clean = company_name.lower()
    company_clean = re.sub(r"\blimited\b|\bltd\b|\bplc\b|\bllp\b", "", company_clean)
    company_clean = re.sub(r"[^a-z0-9]", "", company_clean)

    if not first_name or not company_clean:
        return ""

    return f"{first_name}@{company_clean}.com"


def collect_companies(
    client: RotatingCHClient,
    date_from: str,
    date_to: str,
    seen_companies: set,
    officer_cache: dict,
) -> List[dict]:
    all_rows = []

    for chunk_start, chunk_end in daterange_chunks(
        parse_date(date_from),
        parse_date(date_to),
        chunk_days=7,
    ):
        chunk_from = chunk_start.strftime("%Y-%m-%d")
        chunk_to = chunk_end.strftime("%Y-%m-%d")

        sic_companies = search_sic_companies(client, chunk_from, chunk_to)
        buzzword_companies = search_buzzword_companies(client, chunk_from, chunk_to)

        combined = {}
        for company in sic_companies + buzzword_companies:
            company_number = company.get("company_number")
            if company_number:
                combined[company_number] = company

        for company in combined.values():
            company_number = company.get("company_number")
            company_name = company.get("company_name", "")
            sic_codes = company.get("sic_codes", []) or []
            ro_address = company.get("registered_office_address", {}) or {}
            ro_postcode = ro_address.get("postal_code") or ro_address.get("postcode") or company.get("postcode")

            if not company_number or company_number in seen_companies:
                continue

            if not (sic_matches(sic_codes) or name_has_buzzwords(company_name)):
                continue

            officers = get_company_officers(client, company_number)
            directors = [o for o in officers if is_active_director(o)]

            director_names = []
            director_postcodes = []
            has_target_country = False
            has_multi_appointment_director = False

            for d in directors:
                director_names.append(d.get("name", ""))
                d_postcode = (d.get("address") or {}).get("postal_code", "")
                director_postcodes.append(d_postcode)

                nationality = normalize_country(d.get("nationality"))
                residence = normalize_country(d.get("country_of_residence"))

                if nationality in TARGET_COUNTRIES or residence in TARGET_COUNTRIES:
                    has_target_country = True

                officer_id = get_officer_id(d)
                if officer_id:
                    appt_count = get_officer_appointments_count(client, officer_id, officer_cache)
                    if appt_count > 1:
                        has_multi_appointment_director = True


            row = {
                "company_name": company_name,
                "company_number": company_number,
                "SIC Group": get_sic_group(sic_codes, company_name),
                "Directors": len(directors),
                "Postcode": trim_postcode_area(ro_postcode),
                "In Target Postcode": postcode_prefix_matches(ro_postcode),
                "international?": has_target_country,
                "Serial Founder": has_multi_appointment_director,
            }

            all_rows.append(row)
            seen_companies.add(company_number)

    return all_rows


def write_results_csv(rows: List[dict], filename: str):
    if not rows:
        return

    new_df = pd.DataFrame(rows)

    if os.path.exists(filename):
        try:
            existing_df = pd.read_csv(filename)
        except Exception:
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()

    combined = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df

    if "company_number" in combined.columns:
        combined = combined.drop_duplicates(subset=["company_number"], keep="last")
    else:
        combined = combined.drop_duplicates()

    combined.to_csv(filename, index=False, encoding="utf-8-sig")


def load_results_df() -> pd.DataFrame:
    if os.path.exists(RESULTS_FILE):
        try:
            return pd.read_csv(RESULTS_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def prepare_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    display_df = df.copy()
    display_df = display_df.drop(columns=["company_number"], errors="ignore")

    ordered_cols = [
        "company_name",
        "SIC Group",
        "Directors",
        "Postcode",
        "In Target Postcode",
        "international?",
        "Serial Founder",
    ]
    dynamic_cols = [c for c in display_df.columns if c not in ordered_cols]
    final_cols = [c for c in ordered_cols if c in display_df.columns] + dynamic_cols

    display_df = display_df[final_cols]

    rename_map = {
        "company_name": "Company Name",
    }
    return display_df.rename(columns=rename_map)


def run_pipeline(api_keys: List[str], date_from: str, date_to: str):
    seen_companies = set(load_json_file(SEEN_FILE, []))
    officer_cache = load_json_file(OFFICER_CACHE_FILE, {})

    client = RotatingCHClient(api_keys, rotate_every=599)
    rows = collect_companies(client, date_from, date_to, seen_companies, officer_cache)

    save_json_file(SEEN_FILE, sorted(seen_companies))
    save_json_file(OFFICER_CACHE_FILE, officer_cache)
    write_results_csv(rows, RESULTS_FILE)

    return rows


def build_copy_button_html(text_to_copy: str, button_label: str = "Copy") -> str:
    safe_display_text = html.escape(text_to_copy or "", quote=False)
    safe_input_value = html.escape(text_to_copy or "", quote=True)
    safe_button_label = html.escape(button_label, quote=True)

    template = """
    <html>
      <head>
        <meta charset="UTF-8">
        <style>
          body {{ margin: 0; font-family: 'Source Sans Pro', sans-serif; background: transparent; }}
          .wrap {{ display: flex; align-items: center; gap: 8px; width: 100%; overflow: hidden; padding: 2px 0; }}
          .name {{ flex: 1; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 14px; line-height: 1.3; }}
          button {{ border: 1px solid #991b1b; border-radius: 8px; background: #dc2626; color: white; padding: 4px 10px; font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap; }}
          button:hover {{ background: #b91c1c; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="name" title="__TITLE__">__TEXT__</div>
          <button id="copyButton" type="button">__BUTTON__</button>
        </div>
        <input id="textToCopy" value="__VALUE__" style="position:absolute;left:-9999px;top:-9999px;" />
        <script>
          const copyButton = document.getElementById('copyButton');
          const textToCopy = document.getElementById('textToCopy');
          async function copyToClipboard() {{
            try {{
              await navigator.clipboard.writeText(textToCopy.value);
            }} catch (err) {{
              textToCopy.select();
              document.execCommand('copy');
            }}
            const originalLabel = copyButton.textContent;
            copyButton.textContent = 'Copied';
            setTimeout(() => {{ copyButton.textContent = originalLabel; }}, 1000);
          }}
          copyButton.addEventListener('click', copyToClipboard);
        </script>
      </body>
    </html>
    """
    return (
        template
        .replace('__TITLE__', safe_input_value)
        .replace('__TEXT__', safe_display_text)
        .replace('__BUTTON__', safe_button_label)
        .replace('__VALUE__', safe_input_value)
    )


def render_copy_company_name(company_name: str, company_number: str):
    components.html(build_copy_button_html(company_name, "Copy"), height=42)


def render_interactive_results(df: pd.DataFrame):
    if df.empty:
        st.info("No results yet.")
        return

    st.markdown("### Results")

    header_cols = st.columns([3.0, 1.4, 0.8, 1.0, 1.0, 1.0])
    headers = [
        "Company Name", "SIC Group", "Directors",
        "Postcode", "Intl", "Serial Founder"
    ]
    for col, label in zip(header_cols, headers):
        col.markdown(f"**{label}**")

    st.divider()

    for _, row in df.iterrows():
        cols = st.columns([3.0, 1.4, 0.8, 1.0, 1.0, 1.0])

        with cols[0]:
            render_copy_company_name(str(row.get("company_name", "")), str(row.get("company_number", "")))
        with cols[1]:
            st.write(row.get("SIC Group", ""))
        with cols[2]:
            st.write(row.get("Directors", ""))
        with cols[3]:
            st.write(row.get("Postcode", ""))
        with cols[4]:
            st.write("Yes" if bool(row.get("international?", False)) else "No")
        with cols[5]:
            st.write("Yes" if bool(row.get("Serial Founder", False)) else "No")

        st.divider()


def main():
    st.set_page_config(page_title="Companies House Finder", layout="wide")
    st.title("Companies House Finder")
    st.caption("Search newly incorporated companies and copy company names with one click.")

    api_keys = get_api_keys_from_sources()

    with st.sidebar:
        st.header("Controls")
        default_to = datetime.utcnow().date()
        default_from = default_to - timedelta(days=7)

        date_from = st.date_input("Date from", value=default_from)
        date_to = st.date_input("Date to", value=default_to)
        auto_refresh = st.checkbox("Auto refresh page", value=False)
        auto_run = st.checkbox("Run pipeline on refresh", value=False)

        if auto_refresh:
            inject_auto_refresh(AUTO_REFRESH_SECONDS)
            st.caption(f"Refreshing every {AUTO_REFRESH_SECONDS} seconds.")

        st.markdown("---")
        st.write(f"API keys loaded: {len(api_keys)}")

    if not api_keys:
        st.error("No Companies House API keys found. Set COMPANIES_HOUSE_API_KEYS in Streamlit secrets or environment variables.")
        st.stop()

    run_now = st.button("Run pipeline now", type="primary")
    should_auto_run = auto_refresh and auto_run

    if run_now or should_auto_run:
        with st.spinner("Running Companies House pipeline..."):
            rows = run_pipeline(api_keys, str(date_from), str(date_to))
        st.success(f"Pipeline completed. New companies added: {len(rows)}")

    results_df = load_results_df()

    if not results_df.empty:
        st.download_button(
            "Download CSV",
            data=results_df.to_csv(index=False).encode("utf-8-sig"),
            file_name=RESULTS_FILE,
            mime="text/csv",
        )

    tab1, tab2 = st.tabs(["Interactive view", "Plain dataframe view"])

    with tab1:
        render_interactive_results(results_df)

    with tab2:
        st.dataframe(prepare_display_df(results_df), width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
