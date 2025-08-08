from flask import Flask, request
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

app = Flask(__name__)

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

    # חיבור ל-Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open("קמפיינים יומיים מבסיס נתוני רייטינג פלוס v24.7.25.13.07").sheet1
    sheet.clear()
    sheet.insert_row(headers, 2)
    sheet.insert_rows(rows, 3)

    return "Success", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
