import json
import os
import time
import html
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.company-information.service.gov.uk"
AUTO_REFRESH_SECONDS = 3
REQUEST_TIMEOUT = (5, 20)
MAX_RESULTS_PER_PAGE = 5000

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

SEEN_FILE = "seen_companies.json"
RESULTS_FILE = "companies_house_results.csv"

SIC_GROUP_MAP = {**{code: "Tech" for code in TECH_SIC_CODES}, **{code: "Holdings" for code in HOLDINGS_SIC_CODES}}


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

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _rotate_key_if_needed(self):
        if self.request_count_on_key >= self.rotate_every:
            self.key_index = (self.key_index + 1) % len(self.api_keys)
            self.request_count_on_key = 0

    def _auth(self) -> Tuple[str, str]:
        return (self.api_keys[self.key_index], "")

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        retries = 0
        while retries < len(self.api_keys) + 2:
            self._rotate_key_if_needed()
            url = f"{BASE_URL}{path}"
            resp = self.session.get(url, params=params, auth=self._auth(), timeout=REQUEST_TIMEOUT)
            self.request_count_on_key += 1

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return {}
            if resp.status_code == 429:
                self.key_index = (self.key_index + 1) % len(self.api_keys)
                self.request_count_on_key = 0
                retries += 1
                time.sleep(1)
                continue
            if 500 <= resp.status_code < 600:
                retries += 1
                time.sleep(1)
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


def trim_postcode_area(postcode: Optional[str]) -> str:
    if not postcode:
        return ""
    postcode = postcode.strip().upper()
    return postcode[:-3].strip() if len(postcode) > 3 else postcode


def advanced_search_companies(client: RotatingCHClient, params: dict) -> List[dict]:
    results = []
    start_index = 0

    while True:
        page_params = dict(params)
        page_params["size"] = MAX_RESULTS_PER_PAGE
        page_params["start_index"] = start_index
        data = client.get("/advanced-search/companies", params=page_params)
        items = data.get("items", [])
        if not items:
            break

        results.extend(items)

        if len(items) < MAX_RESULTS_PER_PAGE:
            break

        start_index += MAX_RESULTS_PER_PAGE
        if start_index >= 10000:
            break

    return results


def search_sic_companies(client: RotatingCHClient, screening_date: str) -> List[dict]:
    return advanced_search_companies(
        client,
        {
            "incorporated_from": screening_date,
            "incorporated_to": screening_date,
            "sic_codes": ",".join(sorted(TARGET_SIC_CODES)),
        },
    )


def search_buzzword_companies(client: RotatingCHClient, screening_date: str) -> List[dict]:
    results = []
    seen_numbers = set()
    for term in BUZZWORD_TERMS:
        items = advanced_search_companies(
            client,
            {
                "incorporated_from": screening_date,
                "incorporated_to": screening_date,
                "company_name_includes": term,
            },
        )
        for item in items:
            company_number = item.get("company_number")
            if company_number and company_number not in seen_numbers:
                seen_numbers.add(company_number)
                results.append(item)
    return results


def summarise_company(company: dict) -> Optional[dict]:
    company_number = company.get("company_number")
    company_name = company.get("company_name", "")
    sic_codes = company.get("sic_codes", []) or []
    ro_address = company.get("registered_office_address", {}) or {}
    ro_postcode = ro_address.get("postal_code") or ro_address.get("postcode") or company.get("postcode")

    if not company_number:
        return None
    if not (sic_matches(sic_codes) or name_has_buzzwords(company_name)):
        return None

    return {
        "company_name": company_name,
        "company_number": company_number,
        "SIC Group": get_sic_group(sic_codes, company_name),
        "Postcode": trim_postcode_area(ro_postcode),
    }


def collect_companies(client: RotatingCHClient, screening_date: str, seen_companies: set, progress_bar=None, progress_text=None) -> List[dict]:
    all_rows = []

    sic_companies = search_sic_companies(client, screening_date)
    if progress_bar:
        progress_bar.progress(25, text="Fetched SIC-based company results")

    buzzword_companies = search_buzzword_companies(client, screening_date)
    if progress_bar:
        progress_bar.progress(55, text="Fetched buzzword-based company results")

    combined = {}
    for company in sic_companies:
        company_number = company.get("company_number")
        if company_number and company_number not in seen_companies:
            combined[company_number] = company
    for company in buzzword_companies:
        company_number = company.get("company_number")
        if company_number and company_number not in seen_companies:
            combined[company_number] = company

    total = len(combined)
    if total == 0:
        if progress_bar:
            progress_bar.progress(100, text="No new companies found for this date")
        return []

    for i, company in enumerate(combined.values(), start=1):
        row = summarise_company(company)
        if row is None:
            continue
        all_rows.append(row)
        seen_companies.add(row["company_number"])
        if progress_bar:
            pct = 55 + int((i / total) * 45)
            progress_bar.progress(min(pct, 100), text=f"Processing new companies: {i}/{total}")

    return all_rows


def write_results_csv(rows: List[dict], filename: str):
    if not rows:
        return

    new_df = pd.DataFrame(rows)
    existing_df = pd.read_csv(filename) if os.path.exists(filename) else pd.DataFrame()
    combined = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
    combined = combined.drop_duplicates(subset=["company_number"], keep="last")
    combined.to_csv(filename, index=False, encoding="utf-8-sig")


def load_results_df() -> pd.DataFrame:
    if os.path.exists(RESULTS_FILE):
        try:
            return pd.read_csv(RESULTS_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def filter_results_by_date(df: pd.DataFrame, screening_date: str) -> pd.DataFrame:
    if df.empty or "company_number" not in df.columns:
        return df
    return df


def prepare_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    display_df = df.copy().drop(columns=["company_number"], errors="ignore")
    ordered_cols = ["company_name", "SIC Group", "Postcode"]
    dynamic_cols = [c for c in display_df.columns if c not in ordered_cols]
    display_df = display_df[[c for c in ordered_cols if c in display_df.columns] + dynamic_cols]
    return display_df.rename(columns={"company_name": "Company Name"})


def run_pipeline(api_keys: List[str], screening_date: str, progress_bar=None):
    seen_companies = set(load_json_file(SEEN_FILE, []))
    previous_seen_count = len(seen_companies)
    client = RotatingCHClient(api_keys, rotate_every=599)

    started = time.time()
    rows = collect_companies(client, screening_date, seen_companies, progress_bar=progress_bar)

    save_json_file(SEEN_FILE, sorted(seen_companies))
    write_results_csv(rows, RESULTS_FILE)

    elapsed = round(time.time() - started, 2)
    new_count = len(seen_companies) - previous_seen_count
    return rows, elapsed, new_count


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
          .wrap {{ display: flex; flex-direction: row; align-items: center; justify-content: space-between; gap: 8px; width: 100%; padding: 2px 0; box-sizing: border-box; }}
          .name {{ flex: 1 1 auto; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 14px; line-height: 1.3; }}
          button {{ flex: 0 0 auto; margin-left: auto; border: 1px solid #991b1b; border-radius: 8px; background: #dc2626; color: white; padding: 4px 10px; font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap; display: inline-flex; align-items: center; justify-content: center; }}
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


def render_copy_company_name(company_name: str):
    components.html(build_copy_button_html(company_name, "Copy"), height=42)


def render_interactive_results(df: pd.DataFrame):
    if df.empty:
        st.info("No results yet.")
        return

    st.markdown("### Results")
    header_cols = st.columns([4.0, 1.8, 1.2])
    headers = ["Company Name", "SIC Group", "Postcode"]
    for col, label in zip(header_cols, headers):
        col.markdown(f"**{label}**")

    st.divider()
    for _, row in df.iterrows():
        cols = st.columns([4.0, 1.8, 1.2])
        with cols[0]:
            render_copy_company_name(str(row.get("company_name", "")))
        with cols[1]:
            st.write(row.get("SIC Group", ""))
        with cols[2]:
            st.write(row.get("Postcode", ""))
        st.divider()


def main():
    st.set_page_config(page_title="Companies House Finder", layout="wide")
    st.title("Companies House Finder")
    st.caption("Single-day screening with faster result refresh and one-click company-name copy.")

    if "last_new_results" not in st.session_state:
        st.session_state.last_new_results = 0
    if "last_runtime" not in st.session_state:
        st.session_state.last_runtime = None
    if "last_screening_date" not in st.session_state:
        st.session_state.last_screening_date = None

    api_keys = get_api_keys_from_sources()

    with st.sidebar:
        st.header("Controls")
        screening_date = st.date_input("Screening date", value=datetime.utcnow().date())
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

    metric_cols = st.columns(3)
    metric_cols[0].metric("New results since refresh", st.session_state.last_new_results)
    metric_cols[1].metric("Last runtime (sec)", st.session_state.last_runtime if st.session_state.last_runtime is not None else "-")
    metric_cols[2].metric("Last screened date", str(st.session_state.last_screening_date) if st.session_state.last_screening_date else "-")

    progress_bar = st.progress(0, text="Waiting to run screen")

    run_now = st.button("Run screen", type="primary")
    should_auto_run = auto_refresh and auto_run

    if run_now or should_auto_run:
        with st.spinner("Running single-day Companies House screen..."):
            rows, elapsed, new_count = run_pipeline(api_keys, str(screening_date), progress_bar=progress_bar)
        st.session_state.last_new_results = new_count
        st.session_state.last_runtime = elapsed
        st.session_state.last_screening_date = str(screening_date)
        progress_bar.progress(100, text=f"Completed. New results found: {new_count}")
        st.success(f"Screen completed. New companies added: {len(rows)}. Runtime: {elapsed} seconds.")
    else:
        progress_bar.progress(min(st.session_state.last_new_results, 100) if st.session_state.last_new_results else 0, text="Ready to run screen")

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
