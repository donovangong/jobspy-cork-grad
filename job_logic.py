import csv
import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import pandas as pd
from jobspy import scrape_jobs

BASE_DIR = Path(os.getenv('JOBSPY_OUTPUT_DIR', '/tmp/jobspy-output'))
BASE_DIR.mkdir(parents=True, exist_ok=True)

SENDER = os.getenv('GMAIL_SENDER', '')
APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD', '')
RECIPIENT = os.getenv('GMAIL_RECIPIENT', '')

SEARCH_TERMS = [
    'graduate devops',
    'graduate sre',
    'graduate platform engineer',
    'graduate cloud engineer',
    'graduate infrastructure engineer',
    'graduate production support',
    'graduate site reliability engineer',
    'graduate systems engineer',
]

NEGATIVE_HINTS = [
    'senior', 'staff', 'principal', 'manager', 'director', 'lead ', '5+ years',
    '7+ years', '8+ years', '10+ years', 'architect', 'headhunter', 'recruiter only'
]
POSITIVE_HINTS = [
    'graduate', 'new grad', 'entry level', 'entry-level', 'junior', 'early career',
    'associate', 'recent graduate'
]
ROLE_HINTS = [
    'devops', 'sre', 'site reliability', 'platform', 'cloud', 'infrastructure',
    'production support', 'systems engineer', 'operations engineer'
]


def normalize_text(value: object) -> str:
    if value is None:
        return ''
    return str(value).strip().lower()


def safe_col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name].fillna('')
    return pd.Series([''] * len(df), index=df.index)


def score_row(row: pd.Series) -> tuple[int, list[str]]:
    title = normalize_text(row.get('title', ''))
    desc = normalize_text(row.get('description', ''))
    loc = normalize_text(row.get('location', ''))
    city = normalize_text(row.get('city', ''))
    site = normalize_text(row.get('site', ''))

    text = ' '.join([title, desc, loc, city, site])
    score = 0
    reasons = []

    if 'cork' in text:
        score += 35
        reasons.append('Cork match')
    if any(k in text for k in POSITIVE_HINTS):
        score += 30
        reasons.append('Graduate/entry-level signal')
    if any(k in title for k in ROLE_HINTS):
        score += 25
        reasons.append('Role family match')
    if any(k in desc for k in ['kubernetes', 'linux', 'cloud', 'aws', 'azure', 'python', 'automation', 'monitoring', 'incident']):
        score += 10
        reasons.append('Relevant tech keywords')
    if any(k in text for k in ['sponsorship', 'visa', 'critical skills', 'permit']):
        score += 5
        reasons.append('Possible visa signal')
    if any(k in text for k in NEGATIVE_HINTS):
        score -= 40
        reasons.append('Senior-level signal')
    if 'intern' in text:
        score -= 15
        reasons.append('Internship signal')
    if 'cork' not in text:
        score -= 30
        reasons.append('Non-Cork location')

    return score, reasons


def dedupe_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    key_parts = [
        safe_col(df, 'site').astype(str).str.lower(),
        safe_col(df, 'title').astype(str).str.lower(),
        safe_col(df, 'company').astype(str).str.lower(),
        safe_col(df, 'location').astype(str).str.lower(),
    ]
    df = df.copy()
    df['dedupe_key'] = (key_parts[0] + '|' + key_parts[1] + '|' + key_parts[2] + '|' + key_parts[3]).str.replace(r'\s+', ' ', regex=True)
    df = df.sort_values(['match_score', 'date_posted'], ascending=[False, False], na_position='last')
    df = df.drop_duplicates(subset=['dedupe_key']).drop(columns=['dedupe_key'])
    return df


def fetch_site_jobs(site_name: str, search_term: str) -> pd.DataFrame:
    common_kwargs = dict(
        site_name=[site_name],
        search_term=search_term,
        location='Cork, Ireland',
        results_wanted=40,
        verbose=1,
    )

    if site_name == 'indeed':
        jobs = scrape_jobs(
            **common_kwargs,
            hours_old=24,
            country_indeed='Ireland',
        )
    elif site_name == 'linkedin':
        jobs = scrape_jobs(
            **common_kwargs,
            hours_old=24,
            linkedin_fetch_description=False,
        )
    else:
        raise ValueError(f'Unsupported site: {site_name}')

    if jobs is None:
        return pd.DataFrame()
    if not isinstance(jobs, pd.DataFrame):
        jobs = pd.DataFrame(jobs)
    return jobs


def collect_jobs() -> pd.DataFrame:
    frames = []
    for site in ['indeed', 'linkedin']:
        for term in SEARCH_TERMS:
            try:
                df = fetch_site_jobs(site, term)
                if not df.empty:
                    df['source_search_term'] = term
                    frames.append(df)
            except Exception as exc:
                print(f'[WARN] {site} / {term}: {exc}')

    if not frames:
        return pd.DataFrame()

    all_jobs = pd.concat(frames, ignore_index=True)
    return all_jobs


def filter_and_rank(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, df

    df = df.copy()
    df = df.rename(columns={c: c.lower() for c in df.columns})

    for needed in ['title', 'company', 'location', 'description', 'site', 'date_posted', 'job_url']:
        if needed not in df.columns:
            df[needed] = ''

    scored = df.apply(score_row, axis=1, result_type='expand')
    df['match_score'] = scored[0]
    df['match_reasons'] = scored[1].apply(lambda x: '; '.join(x))

    combined_text = (
        safe_col(df, 'title').astype(str) + ' ' +
        safe_col(df, 'description').astype(str) + ' ' +
        safe_col(df, 'location').astype(str)
    ).str.lower()

    keep_mask = (
        combined_text.str.contains('cork', na=False)
        & combined_text.str.contains(r'graduate|new grad|entry level|entry-level|junior|early career|associate|recent graduate', regex=True, na=False)
        & combined_text.str.contains(r'devops|site reliability|\bsre\b|platform|cloud|infrastructure|production support|systems engineer|operations engineer', regex=True, na=False)
    )

    filtered = df[keep_mask].copy()
    filtered = dedupe_jobs(filtered)
    filtered = filtered.sort_values(['match_score', 'date_posted'], ascending=[False, False], na_position='last')
    top20 = filtered.head(20).copy()
    return filtered, top20


def save_outputs(all_jobs: pd.DataFrame, ranked: pd.DataFrame, top20: pd.DataFrame) -> dict[str, Path]:
    run_ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    paths = {
        'all_jobs': BASE_DIR / f'all_jobs_{run_ts}.csv',
        'ranked_jobs': BASE_DIR / f'ranked_jobs_{run_ts}.csv',
        'top20_jobs': BASE_DIR / f'top20_jobs_{run_ts}.csv',
    }
    all_jobs.to_csv(paths['all_jobs'], index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
    ranked.to_csv(paths['ranked_jobs'], index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
    top20.to_csv(paths['top20_jobs'], index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
    return paths


def send_email(paths: dict[str, Path], ranked_count: int, top_count: int) -> None:
    if not SENDER or not APP_PASSWORD or not RECIPIENT:
        print('[INFO] Gmail env vars missing; skip sending email.')
        return

    msg = EmailMessage()
    msg['Subject'] = f'JobSpy daily Cork graduate jobs - {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
    msg['From'] = SENDER
    msg['To'] = RECIPIENT
    msg.set_content(
        f'Daily JobSpy run finished.\n\n'
        f'Ranked matches: {ranked_count}\n'
        f'Top shortlist: {top_count}\n\n'
        f'Attached: raw, ranked, and top20 CSV files.'
    )

    for path in paths.values():
        with open(path, 'rb') as f:
            data = f.read()
        msg.add_attachment(data, maintype='text', subtype='csv', filename=path.name)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER, APP_PASSWORD)
        smtp.send_message(msg)


def run_once() -> dict:
    all_jobs = collect_jobs()
    if all_jobs.empty:
        return {'ok': False, 'message': 'No jobs collected.', 'all_jobs': 0, 'ranked': 0, 'top20': 0}

    ranked, top20 = filter_and_rank(all_jobs)
    paths = save_outputs(all_jobs, ranked, top20)
    send_email(paths, len(ranked), len(top20))

    return {
        'ok': True,
        'message': 'Run finished.',
        'all_jobs': len(all_jobs),
        'ranked': len(ranked),
        'top20': len(top20),
        'files': {k: str(v) for k, v in paths.items()},
    }
