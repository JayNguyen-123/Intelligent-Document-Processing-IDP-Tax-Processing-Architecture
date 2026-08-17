import os
import base64
import json
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Retrieve operational communication keys from environment variables
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
FROM_EMAIL = os.environ.get("FROM_EMAIL")
TO_EMAIL = os.environ.get("TO_EMAIL")

def handle_hitl_alert(event, context):
    """Event-driven Cloud Function triggered by a Pub/Sub topic subscription."""
    try:
        # Decode the incoming Pub/Sub bytecode payload
        if 'data' not in event:
            print("❌ Error: Received Pub/Sub event envelope with missing payload data block.")
            return

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        alert_data = json.loads(pubsub_message)
        
        file_name = alert_data.get("file_name", "unknown_document.pdf")
        flagged_fields = alert_data.get("flagged_fields", [])
        
        print(f"🚨 Processing operational alert event for file: {file_name}")

        # Execute communications tasks
        if SLACK_WEBHOOK_URL:
            send_slack_notification(file_name, flagged_fields)
        else:
            print("⚠️ Skipped Slack dispatch: SLACK_WEBHOOK_URL variable is not configured.")

        if SENDGRID_API_KEY and FROM_EMAIL and TO_EMAIL:
            send_email_notification(file_name, flagged_fields)
        else:
            print("⚠️ Skipped email dispatch: SendGrid configuration variables are incomplete.")

    except Exception as error:
        print(f"❌ Critical failure handling pipeline notification alert: {str(error)}")

def send_slack_notification(file_name, flagged_fields):
    """Formulates a structural rich text block message and posts to Slack channel."""
    fields_list_str = ", ".join([f"`{field}`" for field in flagged_fields]) if flagged_fields else "General layout error"
    
    slack_payload = {
        "text": f"🚨 *Manual Tax Review Required!*",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🚨 *Manual Tax Review Required!*\n• *File Name:* {file_name}\n• *Flagged Low-Confidence Fields:* {fields_list_str}\n\n👉 _Please log into the Cloud Run HITL Panel to verify and approve this data._"
                }
            }
        ]
    }
    
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL, 
            json=slack_payload, 
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        if response.status_code == 200:
            print("✅ Operational alert successfully dispatched to Slack channel.")
        else:
            print(f"❌ Slack API endpoint rejected notification payload: Code {response.status_code} - {response.text}")
    except Exception as slack_err:
        print(f"❌ Failed to reach Slack webhook connection interface: {str(slack_err)}")

def send_email_notification(file_name, flagged_fields):
    """Compiles a semantic HTML document layout and sends via SendGrid mail engine."""
    fields_list_html = "".join([f"<li><code>{field}</code></li>" for field in flagged_fields]) if flagged_fields else "<li>General extraction drop</li>"
    
    html_content = f"""
    <h3>Intelligent Document Processing Queue Alert</h3>
    <p>The IDP automation pipeline has flagged a document for manual data validation.</p>
    <ul>
        <li><strong>Source Filename:</strong> {file_name}</li>
        <li><strong>Low Confidence Entities Identified:</strong></li>
    </ul>
    <ul>
        {fields_list_html}
    </ul>
    <p>Please log into your Google Cloud Run Human-in-the-Loop review workspace to resolve this issue and stream the verified records into BigQuery.</p>
    """
    
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=TO_EMAIL,
        subject=f"[Action Required] HITL Extraction Queue Alert: {file_name}",
        html_content=html_content
    )
    
    try:
        sg_client = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg_client.send(message)
        if response.status_code in:
            print("✅ Operational alert email successfully transmitted via SendGrid engine.")
        else:
            print(f"❌ SendGrid server rejected transmission block: Code {response.status_code}")
    except Exception as email_err:
        print(f"❌ Failed to process transmission routine through SendGrid infrastructure: {str(email_err)}")
