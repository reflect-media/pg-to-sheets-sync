from flask import Flask, request, jsonify
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import secretmanager
import os
import tempfile
import logging
import gc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def get_secret(secret_id, project_id):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to get secret {secret_id}: {str(e)}")
        raise

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route("/test-db", methods=["GET"])
def test_db():
    try:
        conn = psycopg2.connect(
            host='rtngplsadmin40.data-driven.media',
            port=5432,
            dbname='clients_managment',
            user='looker_mediaforest',
            password=os.environ.get('PG_PASS', ''),
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM campaign_summary_last_7_days_new")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return jsonify({"status": "db_ok", "row_count": count}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def sync_data():
    import time
    start = time.time()
    
    try:
        logger.info("=== START SYNC ===")
        
        # חיבור ל-PostgreSQL
        conn = psycopg2.connect(
            host='rtngplsadmin40.data-driven.media',
            port=5432,
            dbname='clients_managment',
            user='looker_mediaforest',
            password=os.environ['PG_PASS'],
            connect_timeout=15
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM campaign_summary_last_7_days_new LIMIT 2000")
        
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        
        logger.info(f"Fetched {len(rows)} rows")
        
        # Google Sheets
        creds_json = get_secret("pg-to-sheets-sync-23f33d00064e", "426302689818")
        
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix='.json') as temp:
            temp.write(creds_json)
            temp.flush()
            temp_name = temp.name
        
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_name, scope)
        client = gspread.authorize(creds)
        sheet = client.open("קמפיינים יומיים מבסיס נתוני רייטינג פלוס v24.7.25.13.07").sheet1
        
        sheet.clear()
        sheet.insert_row(headers, 1)
        
        if rows:
            batch_size = 100
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                batch_lists = [list(row) for row in batch]
                sheet.insert_rows(batch_lists, i + 2)
                logger.info(f"Inserted batch {i//batch_size + 1}")
                time.sleep(0.1)
        
        os.unlink(temp_name)
        del rows
        gc.collect()
        
        duration = time.time() - start
        logger.info(f"SUCCESS! Time: {duration:.1f}s")
        
        return jsonify({
            "status": "success",
            "rows_processed": len(headers) if headers else 0,
            "duration": f"{duration:.1f}s"
        }), 200
        
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
