import os
import base64
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import requests
from jobspy import scrape_jobs


SEARCH_TERMS = [
    "graduate",
    "graduate programme",
    "entry level",
    "junior",
    "trainee",
    "associate",
    "new grad",
]

EXCLUDE_TERMS = [
    "senior", "staff", "principal", "lead", "manager", "director", "vp", "head"
]

DOCS_DIR = os.getenv("GITHUB_DOCS_DIR", "docs")
GITHUB_REPO = os.getenv("GITHUB_REPO", "").strip()
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
    
def clean_description(text: Any) -> str:
    text = normalize_text(text)
    if not text:
        return ""
    return " ".join(text.split())

def scrape_all_jobs() -> pd.DataFrame:
    frames = []

    for term in SEARCH_TERMS:
        for site in ["indeed", "linkedin"]:
            try:
                jobs = scrape_jobs(
                    site_name=[site],
                    search_term=term,
                    location="Dublin, Ireland",
                    results_wanted=100,
                    hours_old=24,
                    country_indeed="Ireland",
                    linkedin_fetch_description=(site == "linkedin"),
                )
                if jobs is not None and not jobs.empty:
                    jobs = jobs.copy()
                    jobs["search_term"] = term
                    frames.append(jobs)
            except Exception as e:
                print(f"Scrape failed for {site} / {term}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    for col in ["title", "company", "location", "date_posted", "description", "job_url"]:
        if col not in df.columns:
            df[col] = ""

    df["title"] = df["title"].fillna("")
    df["company"] = df["company"].fillna("")
    df["location"] = df["location"].fillna("")
    df["description"] = df["description"].fillna("")
    df["job_url"] = df["job_url"].fillna("")
    df["date_posted"] = df["date_posted"].fillna("")


    df["dedupe_key"] = (
        df["title"].str.lower().str.strip()
        + " | "
        + df["company"].str.lower().str.strip()
        + " | "
        + df["location"].str.lower().str.strip()
    )
    df = df.drop_duplicates(subset=["dedupe_key"])

    df["description"] = df["description"].apply(clean_description)

    # 只保留展示需要的列
    keep_cols = ["title", "company", "location", "date_posted", "description", "job_url"]
    for c in keep_cols:
        if c not in df.columns:
            df[c] = ""

    df = df[keep_cols].sort_values(by=["date_posted", "title"], ascending=[False, True])

    return df


def build_html(df: pd.DataFrame, generated_at: str) -> str:
    if df.empty:
        rows_html = """
        <tr>
          <td colspan="4">No jobs found in the last 24 hours.</td>
        </tr>
        """
    else:
        rows = []
        for _, row in df.iterrows():
            title = normalize_text(row.get("title"))
            company = normalize_text(row.get("company"))
            location = normalize_text(row.get("location"))
            posted = normalize_text(row.get("date_posted"))
            url = normalize_text(row.get("job_url"))

            safe_url = url if url else "#"
            title_html = (
                f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer">{title}</a>'
                if url else title
            )

            rows.append(f"""
            <tr>
              <td>{title_html}</td>
              <td>{company}</td>
              <td>{location}</td>
              <td>{posted}</td>
            </tr>
            """)

        rows_html = "\n".join(rows)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>JobSpy Dublin Graduate</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 24px;
      line-height: 1.4;
    }}
    h1 {{
      margin-bottom: 6px;
    }}
    .meta {{
      color: #666;
      margin-bottom: 20px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      font-size: 14px;
      table-layout: fixed;
    }}
    th, td {{
      border: 1px solid #ddd;
      padding: 8px;
      vertical-align: top;
      text-align: left;
      word-wrap: break-word;
      overflow-wrap: break-word;
    }}
    th {{
      background: #f5f5f5;
      position: sticky;
      top: 0;
    }}
    th:nth-child(1), td:nth-child(1) {{ width: 34%; }}
    th:nth-child(2), td:nth-child(2) {{ width: 24%; }}
    th:nth-child(3), td:nth-child(3) {{ width: 22%; }}
    th:nth-child(4), td:nth-child(4) {{ width: 20%; }}

    a {{
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
  </style>
</head>
<body>
  <h1>Dublin Graduate Jobs</h1>
  <div class="meta">Last updated: {generated_at} UTC</div>
  <div class="meta">
    <a href="jobs.csv" download>Download filtered CSV</a>
  </div>
  <table>
    <thead>
      <tr>
        <th>Title</th>
        <th>Company</th>
        <th>Location</th>
        <th>Posted</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</body>
</html>
"""


def github_get_file_sha(path: str) -> str | None:
    if not (GITHUB_REPO and GITHUB_TOKEN):
        return None

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}?ref={GITHUB_BRANCH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None


def github_put_file(path: str, content_bytes: bytes, message: str) -> None:
    if not (GITHUB_REPO and GITHUB_TOKEN):
        raise RuntimeError("Missing GITHUB_REPO or GITHUB_TOKEN")

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

    sha = github_get_file_sha(path)
    payload = {
        "message": message,
        "branch": GITHUB_BRANCH,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
    }
    if sha:
        payload["sha"] = sha

    resp = requests.put(url, headers=headers, json=payload, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"GitHub upload failed for {path}: {resp.status_code} {resp.text}")


def run_pipeline() -> Dict[str, Any]:
    raw_df = scrape_all_jobs()
    filtered_df = filter_jobs(raw_df)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    html = build_html(filtered_df, generated_at)
    csv_bytes = filtered_df.to_csv(index=False).encode("utf-8")
    html_bytes = html.encode("utf-8")

    html_path = f"{DOCS_DIR}/index.html"
    csv_path = f"{DOCS_DIR}/jobs.csv"

    github_put_file(
        html_path,
        html_bytes,
        f"Update jobs page at {generated_at} UTC"
    )
    github_put_file(
        csv_path,
        csv_bytes,
        f"Update jobs csv at {generated_at} UTC"
    )

    return {
        "generated_at": generated_at,
        "raw_count": int(len(raw_df)),
        "filtered_count": int(len(filtered_df)),
        "html_path": html_path,
        "csv_path": csv_path,
    }
