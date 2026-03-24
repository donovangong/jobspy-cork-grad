import os
import io
import re
import base64
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
import requests
from jobspy import scrape_jobs


SEARCH_TERMS = [
    "graduate devops",
    "graduate sre",
    "graduate cloud engineer",
    "graduate platform engineer",
    "graduate infrastructure engineer",
    "graduate production support",
    "graduate site reliability engineer",
    "entry level devops",
    "entry level cloud engineer",
    "graduate software engineer"
]

INCLUDE_TERMS = [
    "graduate", "entry level", "junior", "intern", "trainee", "new grad"
]

ROLE_TERMS = [
    "devops", "sre", "site reliability", "cloud", "platform",
    "infrastructure", "production support", "systems", "ops"
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


def contains_any(text: str, keywords: List[str]) -> bool:
    lower = text.lower()
    return any(k in lower for k in keywords)


def score_row(row: pd.Series) -> int:
    title = normalize_text(row.get("title"))
    company = normalize_text(row.get("company"))
    location = normalize_text(row.get("location"))
    description = normalize_text(row.get("description"))

    blob = f"{title} {company} {location} {description}".lower()
    score = 0

    if "cork" in location.lower() or "cork" in blob:
        score += 30

    if contains_any(blob, INCLUDE_TERMS):
        score += 25

    matched_roles = sum(1 for term in ROLE_TERMS if term in blob)
    score += min(matched_roles * 10, 30)

    if "graduate" in title.lower() or "entry" in title.lower() or "junior" in title.lower():
        score += 15

    if contains_any(blob, EXCLUDE_TERMS):
        score -= 100

    return max(score, 0)


def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    for col in ["title", "company", "location", "description", "site", "job_url", "date_posted"]:
        if col not in df.columns:
            df[col] = ""

    df["title"] = df["title"].fillna("")
    df["company"] = df["company"].fillna("")
    df["location"] = df["location"].fillna("")
    df["description"] = df["description"].fillna("")
    df["site"] = df["site"].fillna("")
    df["job_url"] = df["job_url"].fillna("")
    df["date_posted"] = df["date_posted"].fillna("")

    # 只保留 Cork
    df = df[df["location"].str.contains("Cork", case=False, na=False)]

    # 标题或描述里要像 graduate / entry-level
    graduate_mask = (
        df["title"].str.contains(r"graduate|entry|junior|intern|trainee|new grad", case=False, na=False)
        | df["description"].str.contains(r"graduate|entry.?level|junior|intern|trainee|new grad", case=False, na=False)
    )
    df = df[graduate_mask]

    # 排除明显 senior
    exclude_mask = (
        df["title"].str.contains(r"senior|staff|principal|lead|manager|director|head|vp", case=False, na=False)
        | df["description"].str.contains(r"senior|staff|principal|lead|manager|director|head|vp", case=False, na=False)
    )
    df = df[~exclude_mask]

    # 给分
    df["match_score"] = df.apply(score_row, axis=1)

    # 去重
    dedupe_key = (
        df["title"].str.lower().str.strip()
        + " | "
        + df["company"].str.lower().str.strip()
        + " | "
        + df["location"].str.lower().str.strip()
    )
    df["dedupe_key"] = dedupe_key
    df = df.sort_values(by=["match_score"], ascending=False).drop_duplicates(subset=["dedupe_key"])

    # 只保留最有意义的列
    keep_cols = ["match_score", "title", "company", "site", "location", "date_posted", "job_url"]
    for c in keep_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[keep_cols].sort_values(by=["match_score", "date_posted"], ascending=[False, False])

    return df


def scrape_all_jobs() -> pd.DataFrame:
    frames = []

    for term in SEARCH_TERMS:
        for site in ["indeed", "linkedin"]:
            try:
                jobs = scrape_jobs(
                    site_name=[site],
                    search_term=term,
                    location="Cork, Ireland",
                    results_wanted=30,
                    hours_old=24,
                    country_indeed="Ireland",
                )
                if jobs is not None and not jobs.empty:
                    frames.append(jobs)
            except Exception as e:
                print(f"Scrape failed for {site} / {term}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def build_html(df: pd.DataFrame, generated_at: str) -> str:
    if df.empty:
        rows_html = """
        <tr>
          <td colspan="7">No jobs found in the last 24 hours.</td>
        </tr>
        """
    else:
        rows = []
        for _, row in df.iterrows():
            score = int(row.get("match_score", 0))
            score_class = "low"
            if score >= 70:
                score_class = "high"
            elif score >= 50:
                score_class = "mid"

            title = normalize_text(row.get("title"))
            company = normalize_text(row.get("company"))
            site = normalize_text(row.get("site"))
            location = normalize_text(row.get("location"))
            posted = normalize_text(row.get("date_posted"))
            url = normalize_text(row.get("job_url"))

            rows.append(f"""
            <tr>
              <td><span class="score {score_class}">{score}</span></td>
              <td><a href="{url}" target="_blank" rel="noopener noreferrer">{title}</a></td>
              <td>{company}</td>
              <td>{site}</td>
              <td>{location}</td>
              <td>{posted}</td>
              <td><a href="{url}" target="_blank" rel="noopener noreferrer">Open</a></td>
            </tr>
            """)

        rows_html = "\n".join(rows)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>JobSpy Cork Graduate</title>
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
    }}
    th, td {{
      border: 1px solid #ddd;
      padding: 8px;
      vertical-align: top;
      text-align: left;
    }}
    th {{
      background: #f5f5f5;
      position: sticky;
      top: 0;
    }}
    .score {{
      display: inline-block;
      min-width: 42px;
      text-align: center;
      padding: 4px 8px;
      border-radius: 999px;
      font-weight: bold;
    }}
    .high {{ background: #d1fae5; }}
    .mid {{ background: #fef3c7; }}
    .low {{ background: #e5e7eb; }}
    a {{
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
  </style>
</head>
<body>
  <h1>Cork Graduate Jobs</h1>
  <div class="meta">Last updated: {generated_at} UTC</div>
  <table>
    <thead>
      <tr>
        <th>Match</th>
        <th>Title</th>
        <th>Company</th>
        <th>Site</th>
        <th>Location</th>
        <th>Posted</th>
        <th>Link</th>
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
    ranked_df = filter_jobs(raw_df)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    html = build_html(ranked_df, generated_at)
    csv_bytes = ranked_df.to_csv(index=False).encode("utf-8")
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
        "ranked_count": int(len(ranked_df)),
        "html_path": html_path,
        "csv_path": csv_path,
    }
