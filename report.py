import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

def query(sql):
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return result.fetchall()

# --- run queries ---
top_artists = query("""
    SELECT artist_name, COUNT(*) as plays
    FROM plays
    GROUP BY artist_name
    ORDER BY plays DESC
    LIMIT 5
""")

top_tracks = query("""
    SELECT track_name, artist_name, COUNT(*) as plays
    FROM plays
    GROUP BY track_name, artist_name
    ORDER BY plays DESC
    LIMIT 5
""")

total_hours = query("""
    SELECT ROUND(CAST(SUM(duration_s) / 3600 AS numeric), 2) as hours
    FROM plays
""")[0][0]

peak_hour = query("""
    SELECT EXTRACT(HOUR FROM played_at) as hour, COUNT(*) as plays
    FROM plays
    GROUP BY hour
    ORDER BY plays DESC
    LIMIT 1
""")[0][0]

total_tracks = query("SELECT COUNT(*) FROM plays")[0][0]

# --- build email html ---
def rows_to_html(rows, headers):
    header_html = "".join(f"<th style='padding:8px;text-align:left;border-bottom:2px solid #ddd'>{h}</th>" for h in headers)
    rows_html = ""
    for row in rows:
        cells = "".join(f"<td style='padding:8px;border-bottom:1px solid #eee'>{v}</td>" for v in row)
        rows_html += f"<tr>{cells}</tr>"
    return f"<table style='border-collapse:collapse;width:100%'><tr>{header_html}</tr>{rows_html}</table>"

now = datetime.now(timezone.utc).strftime("%B %d, %Y")

html = f"""
<div style='font-family:Arial,sans-serif;max-width:600px;margin:0 auto'>
  <h2 style='color:#1DB954'>🎵 Spotify Listening Report</h2>
  <p style='color:#666'>{now}</p>

  <div style='background:#f9f9f9;padding:16px;border-radius:8px;margin:16px 0'>
    <b>Total plays tracked:</b> {total_tracks}<br>
    <b>Total listening time:</b> {total_hours} hours<br>
    <b>Peak listening hour:</b> {int(peak_hour)}:00
  </div>

  <h3>Top 5 Artists</h3>
  {rows_to_html(top_artists, ["Artist", "Plays"])}

  <h3>Top 5 Tracks</h3>
  {rows_to_html(top_tracks, ["Track", "Artist", "Plays"])}
</div>
"""

# --- send email ---
msg = MIMEMultipart("alternative")
msg["Subject"] = f"Spotify Report — {now}"
msg["From"]    = os.getenv("GMAIL_ADDRESS")
msg["To"]      = os.getenv("REPORT_RECIPIENT")
msg.attach(MIMEText(html, "html"))

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
    server.login(os.getenv("GMAIL_ADDRESS"), os.getenv("GMAIL_APP_PASSWORD"))
    server.send_message(msg)

print(f"report sent to {os.getenv('REPORT_RECIPIENT')}")