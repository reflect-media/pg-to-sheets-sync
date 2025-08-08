from flask import Flask, request
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import secretmanager
import os
import json
import tempfile

app = Flask(__name__)

def get_secret(secret_id, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@app.route("/", methods=["GET"])
def sync_data():
    # חיבור ל-PostgreSQL
    conn = psycopg2.connect(
        host='rtngplsadmin40.data-driven.media',
        port=5432,
        dbname='clients_managment',
        user='looker_mediaforest',
        password=os.environ['PG_PASS']
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM campaign_summary_last_7_days_new")
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    # קבלת קובץ credentials מה-Secret Manager ושמירה זמנית
    creds_json = get_secret("pg-to-sheets-sync-23f33d00064e", "426302689818")
    with tempfile.NamedTemporaryFile("w+", delete=False) as temp:
        temp.write(creds_json)
        temp.flush()
        temp_name = temp.name

    # חיבור ל-Google Sheets
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(temp_name, scope)
    client = gspread.authorize(creds)
    sheet = client.open("קמפיינים יומיים מבסיס נתוני רייטינג פלוס v24.7.25.13.07").sheet1
    sheet.clear()
    sheet.insert_row(headers, 2)
    sheet.insert_rows(rows, 3)

    return "Success", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
