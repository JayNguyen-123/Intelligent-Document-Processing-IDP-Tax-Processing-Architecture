"""HITL alert Cloud Function (2nd gen, Pub/Sub CloudEvent trigger).

Fans a MANUAL_REVIEW_REQUIRED message out to Slack and/or SendGrid email.

* File must be named main.py - Cloud Functions loads the entry point from it.
* Secrets (SLACK_WEBHOOK_URL, SENDGRID_API_KEY) are injected from Secret Manager
  with --set-secrets, never as plain env vars.
* All user-controlled strings are escaped before going into Slack mrkdwn / HTML.
* If every configured channel fails, the function raises so Pub/Sub redelivers
  (deploy with --retry and a dead-letter topic). Malformed messages are logged
  and acked - retrying them can never succeed.
"""
from __future__ import annotations

import base64
import html
import json
import logging
import os
import sys
from typing import Any, Dict, List

import functions_framework
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "").strip()
FROM_EMAIL = os.environ.get("FROM_EMAIL", "").strip()
TO_EMAILS = [e.strip() for e in os.environ.get("TO_EMAIL", "").split(",") if e.strip()]
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "").strip()
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "10"))

_logger = logging.getLogger("alerts")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_h)
    _logger.propagate = False


def log(severity: str, message: str, **fields: Any) -> None:
    _logger.info(json.dumps({"severity": severity, "message": message, "component": "alerts", **fields}, default=str))


class DeliveryError(RuntimeError):
    pass


@functions_framework.cloud_event
def handle_hitl_alert(cloud_event) -> None:
    try:
        alert = decode_message(cloud_event.data or {})
    except (KeyError, ValueError) as exc:
        log("ERROR", "Malformed Pub/Sub message; acknowledging without retry", error=str(exc))
        return

    channels: Dict[str, Any] = {}
    if SLACK_WEBHOOK_URL:
        channels["slack"] = send_slack_notification
    if SENDGRID_API_KEY and FROM_EMAIL and TO_EMAILS:
        channels["email"] = send_email_notification
    if not channels:
        log("WARNING", "No alert channel configured (Slack/SendGrid); alert dropped",
            document_id=alert.get("document_id"))
        return

    failures: Dict[str, str] = {}
    for name, sender in channels.items():
        try:
            sender(alert)
            log("INFO", f"Alert delivered via {name}", document_id=alert.get("document_id"))
        except Exception as exc:  # noqa: BLE001 - isolate channels from each other
            failures[name] = str(exc)
            log("ERROR", f"Alert delivery via {name} failed", error=str(exc), document_id=alert.get("document_id"))

    if len(failures) == len(channels):
        raise DeliveryError(f"All alert channels failed: {failures}")


def decode_message(data: Dict[str, Any]) -> Dict[str, Any]:
    raw = data["message"]["data"]
    alert = json.loads(base64.b64decode(raw).decode("utf-8"))
    if not isinstance(alert, dict) or alert.get("alert_type") != "MANUAL_REVIEW_REQUIRED":
        raise ValueError("unexpected alert payload")
    return alert


def _slack_escape(text: Any) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("`", "'")


def _flagged(alert: Dict[str, Any]) -> List[str]:
    return [str(f) for f in alert.get("flagged_fields", [])][:25]


def build_slack_payload(alert: Dict[str, Any]) -> Dict[str, Any]:
    fields = ", ".join(f"`{_slack_escape(f)}`" for f in _flagged(alert)) or "_none (see reasons)_"
    reasons = ", ".join(_slack_escape(r) for r in alert.get("review_reasons", [])) or "unspecified"
    link = f"\n<{DASHBOARD_URL}|Open the HITL review console>" if DASHBOARD_URL else ""
    text = (
        "*Manual tax document review required*\n"
        f"• *File:* `{_slack_escape(alert.get('file_name', 'unknown'))}`\n"
        f"• *Reasons:* {reasons}\n"
        f"• *Flagged fields:* {fields}\n"
        f"• *Document ID:* `{_slack_escape(alert.get('document_id', ''))}`{link}"
    )
    return {"text": "Manual tax document review required",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]}


def send_slack_notification(alert: Dict[str, Any]) -> None:
    resp = requests.post(SLACK_WEBHOOK_URL, json=build_slack_payload(alert), timeout=HTTP_TIMEOUT)
    if resp.status_code != 200:
        raise DeliveryError(f"Slack returned HTTP {resp.status_code}: {resp.text[:200]}")


def build_email_html(alert: Dict[str, Any]) -> str:
    e = html.escape
    items = "".join(f"<li><code>{e(f)}</code></li>" for f in _flagged(alert)) or "<li>None (see reasons)</li>"
    reasons = e(", ".join(map(str, alert.get("review_reasons", []))) or "unspecified")
    link = f'<p><a href="{e(DASHBOARD_URL, quote=True)}">Open the HITL review console</a></p>' if DASHBOARD_URL else ""
    return (
        "<h3>Tax document requires manual review</h3>"
        "<p>The IDP pipeline could not auto-verify a document.</p>"
        f"<ul><li><strong>File:</strong> {e(str(alert.get('file_name', 'unknown')))}</li>"
        f"<li><strong>Document ID:</strong> {e(str(alert.get('document_id', '')))}</li>"
        f"<li><strong>Reasons:</strong> {reasons}</li></ul>"
        f"<p><strong>Flagged fields:</strong></p><ul>{items}</ul>{link}"
    )


def send_email_notification(alert: Dict[str, Any]) -> None:
    # Subject is plain text; strip CR/LF to prevent header injection.
    file_name = str(alert.get("file_name", "unknown")).replace("\r", " ").replace("\n", " ")[:120]
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=TO_EMAILS,
        subject=f"[Action Required] Tax document review: {file_name}",
        html_content=build_email_html(alert),
    )
    response = SendGridAPIClient(SENDGRID_API_KEY).send(message)
    if response.status_code not in (200, 201, 202):
        raise DeliveryError(f"SendGrid returned HTTP {response.status_code}")
