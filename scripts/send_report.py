from __future__ import annotations

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def main() -> None:
    required = ["SMTP_SERVER", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "REPORT_FROM", "REPORT_TO"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print("Email skipped: SMTP secrets not configured.")
        return

    html = Path("reports/latest_report.html").read_text(encoding="utf-8") if Path("reports/latest_report.html").exists() else ""
    text = Path("reports/latest_report.md").read_text(encoding="utf-8") if Path("reports/latest_report.md").exists() else ""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "EASYGE Monthly Holdings Report"
    msg["From"] = os.environ["REPORT_FROM"]
    msg["To"] = os.environ["REPORT_TO"]

    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(os.environ["SMTP_SERVER"], int(os.environ["SMTP_PORT"])) as server:
        server.starttls()
        server.login(os.environ["SMTP_USERNAME"], os.environ["SMTP_PASSWORD"])
        server.sendmail(os.environ["REPORT_FROM"], [x.strip() for x in os.environ["REPORT_TO"].split(",")], msg.as_string())

    print("Report email sent.")


if __name__ == "__main__":
    main()
