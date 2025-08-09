from flask import Flask, request, jsonify
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import secretmanager
import os
import tempfile
import logging

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
        
        # חיפוש כל הטבלאות/views הזמינות
        cursor.execute("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        
        all_tables = cursor.fetchall()
        
        # חיפוש ספציפי לטבלאות עם נתונים רלוונטיים
        cursor.execute("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%campaign%' 
                 OR table_name LIKE '%summary%'
                 OR table_name LIKE '%data%'
                 OR table_name LIKE '%report%')
            ORDER BY table_name;
        """)
        
        relevant_tables = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "db_connected",
            "total_tables_count": len(all_tables),
            "all_tables": all_tables[:20],  # רק 20 הראשונות
            "relevant_tables": relevant_tables,
            "target_table": "campaign_summary_last_7_days_new",
            "current_user": "looker_mediaforest"
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def sync_data():
    import time
    start = time.time()
    
    try:
        logger.info("=== START SYNC (LIMITED) ===")
        
        # חיבור ל-PostgreSQL
        conn = psycopg2.connect(
            host='rtngplsadmin40.data-driven.media',
            port=5432,
            dbname='clients_managment',
            user='looker_mediaforest',
            password=os.environ['PG_PASS'],
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        # הגבלה קפדנית לחיסכון בזיכרון
        cursor.execute("SELECT * FROM campaign_summary_last_7_days_new LIMIT 500")
        
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        
        logger.info(f"Fetched {len(rows)} rows")
        
        # Google Sheets - מאופטמל לזיכרון
        creds_json = get_secret("pg-to-sheets-sync-23f33d00064e", "426302689818")
        
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix='.json') as temp:
            temp.write(creds_json)
            temp.flush()
            temp_name = temp.name
        
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_name, scope)
        client = gspread.authorize(creds)
        sheet = client.open("קמפיינים יומיים מבסיס נתוני רייטינג פלוס v24.7.25.13.07").sheet1
        
        # ניקוי וכתיבה בחלקים קטנים
        sheet.clear()
        sheet.insert_row(headers, 1)
        
        if rows:
            # באצ'ים קטנים של 50 שורות
            batch_size = 50
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                # המרה לרשימות (חיסכון בזיכרון)
                batch_lists = [list(row) for row in batch]
                sheet.insert_rows(batch_lists, i + 2)
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1}")
                # מנוחה קצרה
                time.sleep(0.2)
                
                # ניקוי זיכרון בכל באץ'
                del batch_lists
        
        # ניקוי
        os.unlink(temp_name)
        del rows, creds_json
        
        duration = time.time() - start
        logger.info(f"SUCCESS! Time: {duration:.1f}s")
        
        return jsonify({
            "status": "success",
            "rows_processed": len(headers) if headers else 0,
            "duration": f"{duration:.1f}s",
            "note": "Limited to 500 rows for memory optimization"
        }), 200
        
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500
@app.route("/test-ip", methods=["GET"])
def test_ip():
    try:
        import requests
        response = requests.get('https://httpbin.org/ip', timeout=10)
        return jsonify({
            "external_ip": response.json(),
            "vpc_connector": "pg-sheets-connector status unknown"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/test-permissions", methods=["GET"])
def test_permissions():
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
        
        # בדיקת הרשאות
        cursor.execute("""
            SELECT grantee, table_name, privilege_type
            FROM information_schema.role_table_grants 
            WHERE grantee = 'looker_mediaforest'
            AND table_name LIKE '%campaign%'
            ORDER BY table_name;
        """)
        
        permissions = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "permissions_check",
            "user_permissions": permissions
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    # הגדרות חיסכון בזיכרון
    app.run(host="0.0.0.0", port=port, debug=False, threaded=False)
