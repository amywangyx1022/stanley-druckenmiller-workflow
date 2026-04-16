#!/usr/bin/env python3
"""Send the generated morning review through Gmail SMTP."""

from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send the morning review email through Gmail SMTP")
    parser.add_argument("--markdown", required=True, help="Path to the generated markdown review")
    parser.add_argument("--snapshot", required=True, help="Path to the market snapshot JSON")
    parser.add_argument("--news", required=True, help="Path to the collected news JSON")
    parser.add_argument("--smtp-host", default="smtp.gmail.com", help="SMTP host")
    parser.add_argument("--smtp-port", type=int, default=465, help="SMTP port")
    parser.add_argument("--smtp-username", default=os.environ.get("GMAIL_SMTP_USERNAME", ""), help="SMTP username")
    parser.add_argument("--app-password", default=os.environ.get("GMAIL_APP_PASSWORD", ""), help="Gmail app password")
    parser.add_argument("--from", dest="from_addr", default=os.environ.get("MORNING_REVIEW_EMAIL_FROM", ""), help="From address")
    parser.add_argument("--to", default=os.environ.get("MORNING_REVIEW_EMAIL_TO", ""), help="Comma-separated To recipients")
    parser.add_argument("--cc", default=os.environ.get("MORNING_REVIEW_EMAIL_CC", ""), help="Comma-separated CC recipients")
    parser.add_argument(
        "--subject-prefix",
        default=os.environ.get("MORNING_REVIEW_SUBJECT_PREFIX", "Morning Review"),
        help="Subject prefix",
    )
    return parser.parse_args()


def parse_recipients(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def load_generated_at(snapshot_path: Path) -> str:
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    raw = payload.get("generated_at_utc")
    if isinstance(raw, str) and raw:
        return raw
    return datetime.now(timezone.utc).isoformat()


def build_message(
    markdown_path: Path,
    snapshot_path: Path,
    news_path: Path,
    smtp_username: str,
    from_addr: str,
    to_addrs: list[str],
    cc_addrs: list[str],
    subject_prefix: str,
) -> EmailMessage:
    generated_at = load_generated_at(snapshot_path)
    review_text = markdown_path.read_text(encoding="utf-8")

    msg = EmailMessage()
    msg["From"] = from_addr or smtp_username
    msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg["Subject"] = f"{subject_prefix} - {generated_at[:10]}"
    msg.set_content(review_text)

    for path, subtype in [
        (markdown_path, "markdown"),
        (snapshot_path, "json"),
        (news_path, "json"),
    ]:
        msg.add_attachment(
            path.read_bytes(),
            maintype="text",
            subtype=subtype,
            filename=path.name,
        )

    return msg


def send_message(host: str, port: int, username: str, app_password: str, message: EmailMessage) -> None:
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(host, port, context=context) as server:
        server.login(username, app_password)
        server.send_message(message)


def main() -> int:
    args = parse_args()
    to_addrs = parse_recipients(args.to)
    cc_addrs = parse_recipients(args.cc)

    if not args.smtp_username:
        raise SystemExit("Missing SMTP username. Set --smtp-username or GMAIL_SMTP_USERNAME.")
    if not args.app_password:
        raise SystemExit("Missing Gmail app password. Set --app-password or GMAIL_APP_PASSWORD.")
    if not to_addrs:
        raise SystemExit("Missing recipient list. Set --to or MORNING_REVIEW_EMAIL_TO.")

    markdown_path = Path(args.markdown)
    snapshot_path = Path(args.snapshot)
    news_path = Path(args.news)
    for path in [markdown_path, snapshot_path, news_path]:
        if not path.exists():
            raise SystemExit(f"Missing required file: {path}")

    message = build_message(
        markdown_path=markdown_path,
        snapshot_path=snapshot_path,
        news_path=news_path,
        smtp_username=args.smtp_username,
        from_addr=args.from_addr,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        subject_prefix=args.subject_prefix,
    )
    send_message(args.smtp_host, args.smtp_port, args.smtp_username, args.app_password, message)
    print(f"Sent morning review email to {', '.join(to_addrs + cc_addrs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
