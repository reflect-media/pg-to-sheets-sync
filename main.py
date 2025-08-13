from flask import Flask, request, jsonify
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import secretmanager
import os
import tempfile
import logging
import json
from google.oauth2 import service_account
import datetime
import decimal
import pytz

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

@app.route("/", methods=["GET"])
def sync_data():
    import time
    start = time.time()
    
    try:
        logger.info("=== START FULL SYNC ===")
        
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
        cursor.execute("SELECT * FROM campaign_summary_last_7_days_new LIMIT 1000")
        
        raw_rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        
        # המרת תאריכים ומספרים עשרוניים לטקסט
        rows = []
        for row in raw_rows:
            converted_row = []
            for item in row:
                if isinstance(item, (datetime.date, datetime.datetime)):
                    converted_row.append(str(item))
                elif isinstance(item, decimal.Decimal):
                    converted_row.append(float(item))
                elif item is None:
                    converted_row.append("")
                else:
                    converted_row.append(item)
            rows.append(converted_row)
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Fetched {len(rows)} rows from PostgreSQL")
        
        # Google Sheets
        creds_dict = json.loads(os.environ['GOOGLE_CREDS_JSON'])
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, 
            scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        )
        client = gspread.authorize(credentials)
        
        sheet = client.open("קמפיינים יומיים מבסיס נתוני רייטינג פלוס v24.7.25.13.07").sheet1
        
        # ניקוי הגיליון
        sheet.clear()
        
        # יצירת שורת מידע מעוצבת עם אזור זמן ישראלי
        israel_tz = pytz.timezone('Asia/Jerusalem')
        current_time = datetime.datetime.now(israel_tz).strftime("%d/%m/%Y %H:%M:%S")
        info_row = [
            f"מקור: PostgreSQL | טבלה: campaign_summary_last_7_days_new | עודכן: {current_time} | שורות: {len(rows)} | סטטוס: הסנכרון הושלם בהצלחה"
        ]
        
        # יצירת נתונים מלאים לעדכון יחיד
        all_data = [info_row, headers] + rows
        
        # עדכון הגיליון בפעולה אחת - אוטומטי לכל כמות עמודות
        sheet.update('A1', all_data)
        
        logger.info(f"✅ Updated sheet with {len(rows)} rows and {len(headers)} columns")
        
        # עיצוב שורת המידע (שורה 1)
        try:
            sheet.format('A1:Z1', {
                'backgroundColor': {
                    'red': 0.8,
                    'green': 0.9,
                    'blue': 1.0
                },
                'textFormat': {
                    'bold': True,
                    'fontSize': 11
                },
                'horizontalAlignment': 'LEFT'
            })
            
            # מיזוג תאים בשורה 1
            sheet.merge_cells(f'A1:{end_col}1')
            
        except Exception as format_error:
            logger.warning(f"Format error (non-critical): {format_error}")
        
            # עיצוב שורת הכותרות (שורה 2)
            format_range = f'A2:{end_col}2'
            sheet.format(format_range, {
                'backgroundColor': {
                    'red': 0.9,
                    'green': 0.9,
                    'blue': 0.9
                },
                'textFormat': {
                    'bold': True,
                    'fontSize': 10
                }
            })
        except Exception as format_error:
            logger.warning(f"Header format error (non-critical): {format_error}")
        
        duration = time.time() - start
        logger.info(f"🎉 COMPLETE SUCCESS! Time: {duration:.1f}s")
        
        return jsonify({
            "status": "🎉 COMPLETE SUCCESS!",
            "rows_processed": len(rows),
            "duration": f"{duration:.1f}s",
            "timestamp": current_time,
            "info_row": info_row[0]
        }), 200
        
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/sync-table2", methods=["GET"])
def sync_table2():
    """סנכרון טבלה campaign_summary_rating_plus_delivery לגיליון CampaignsFullData"""
    import time
    start = time.time()
    
    try:
        logger.info("=== START TABLE2 SYNC (CampaignsFullData) ===")
        
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
        cursor.execute("SELECT * FROM campaign_summary_rating_plus_delivery LIMIT 1000")
        
        raw_rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        
        # המרת תאריכים ומספרים עשרוניים לטקסט
        rows = []
        for row in raw_rows:
            converted_row = []
            for item in row:
                if isinstance(item, (datetime.date, datetime.datetime)):
                    converted_row.append(str(item))
                elif isinstance(item, decimal.Decimal):
                    converted_row.append(float(item))
                elif item is None:
                    converted_row.append("")
                else:
                    converted_row.append(item)
            rows.append(converted_row)
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Fetched {len(rows)} rows from campaign_summary_rating_plus_delivery")
        
        # Google Sheets
        creds_dict = json.loads(os.environ['GOOGLE_CREDS_JSON'])
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, 
            scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        )
        client = gspread.authorize(credentials)
        
        # פתיחת הגיליון CampaignsFullData
        sheet = client.open("קמפיינים יומיים מבסיס נתוני רייטינג פלוס v24.7.25.13.07").worksheet("CampaignsFullData")
        
        # ניקוי הגיליון
        sheet.clear()
        
        # יצירת שורת מידע מעוצבת עם אזור זמן ישראלי
        israel_tz = pytz.timezone('Asia/Jerusalem')
        current_time = datetime.datetime.now(israel_tz).strftime("%d/%m/%Y %H:%M:%S")
        info_row = [
            f"מקור: PostgreSQL | טבלה: campaign_summary_rating_plus_delivery | עודכן: {current_time} | שורות: {len(rows)} | סטטוס: הסנכרון הושלם בהצלחה"
        ]
        
        # יצירת נתונים מלאים לעדכון יחיד
        all_data = [info_row, headers] + rows
        
        # עדכון הגיליון בפעולה אחת
        if len(headers) <= 26:  # A-Z
            end_col = chr(ord('A') + len(headers) - 1)
        else:  # יותר מ-26 עמודות - חישוב נכון עבור AA, AB וכו'
            if len(headers) <= 702:  # עד ZZ
                if len(headers) <= 26:
                    end_col = chr(ord('A') + len(headers) - 1)
                else:
                    first_letter = chr(ord('A') + (len(headers) - 27) // 26)
                    second_letter = chr(ord('A') + (len(headers) - 27) % 26)
                    end_col = first_letter + second_letter
            else:
                end_col = 'ZZ'  # מגבלה מקסימלית
        
        end_row = len(all_data)
        range_to_update = f'A1:{end_col}{end_row}'
        sheet.update(range_to_update, all_data)
        
        logger.info(f"✅ Updated CampaignsFullData sheet with {len(rows)} rows in single operation")
        
        # עיצוב שורת המידע (שורה 1) - רקע ירוק בהיר
        try:
            sheet.format('A1:Z1', {
                'backgroundColor': {
                    'red': 0.8,
                    'green': 1.0,
                    'blue': 0.8
                },
                'textFormat': {
                    'bold': True,
                    'fontSize': 11
                },
                'horizontalAlignment': 'LEFT'
            })
            
            # מיזוג תאים בשורה 1
            sheet.merge_cells(f'A1:{end_col}1')
            
        except Exception as format_error:
            logger.warning(f"Format error (non-critical): {format_error}")
        
        # עיצוב שורת הכותרות (שורה 2)
        try:
            sheet.format('A2:Z2', {
                'backgroundColor': {
                    'red': 0.9,
                    'green': 0.9,
                    'blue': 0.9
                },
                'textFormat': {
                    'bold': True,
                    'fontSize': 10
                }
            })
        except Exception as format_error:
            logger.warning(f"Header format error (non-critical): {format_error}")
        
        duration = time.time() - start
        logger.info(f"🎉 TABLE2 SYNC SUCCESS! Time: {duration:.1f}s")
        
        return jsonify({
            "status": "🎉 TABLE2 SYNC SUCCESS!",
            "table": "campaign_summary_rating_plus_delivery",
            "sheet": "CampaignsFullData",
            "rows_processed": len(rows),
            "duration": f"{duration:.1f}s",
            "timestamp": current_time
        }), 200
        
    except Exception as e:
        logger.error(f"TABLE2 SYNC ERROR: {str(e)}")
        return jsonify({
            "error": str(e), 
            "table": "campaign_summary_rating_plus_delivery",
            "sheet": "CampaignsFullData"
        }), 500

@app.route("/sync-all", methods=["GET"])
def sync_all():
    """סנכרון כל הטבלאות"""
    import time
    start = time.time()
    results = []
    
    logger.info("=== START SYNC ALL TABLES ===")
    
    # סנכרון טבלה 1 - campaign_summary_last_7_days_new
    try:
        # קריאה לפונקציה הקיימת (מחזירה Response object)
        response1 = sync_data()
        if response1[1] == 200:  # status code
            results.append({
                "table": "campaign_summary_last_7_days_new", 
                "sheet": "Sheet1",
                "status": "success"
            })
        else:
            results.append({
                "table": "campaign_summary_last_7_days_new",
                "sheet": "Sheet1", 
                "status": "error"
            })
    except Exception as e:
        results.append({
            "table": "campaign_summary_last_7_days_new",
            "sheet": "Sheet1",
            "status": "error", 
            "error": str(e)
        })
    
    # המתנה קצרה בין סנכרונים
    time.sleep(2)
    
    # סנכרון טבלה 2 - campaign_summary_rating_plus_delivery
    try:
        response2 = sync_table2()
        if response2[1] == 200:
            results.append({
                "table": "campaign_summary_rating_plus_delivery",
                "sheet": "CampaignsFullData", 
                "status": "success"
            })
        else:
            results.append({
                "table": "campaign_summary_rating_plus_delivery",
                "sheet": "CampaignsFullData",
                "status": "error"
            })
    except Exception as e:
        results.append({
            "table": "campaign_summary_rating_plus_delivery", 
            "sheet": "CampaignsFullData",
            "status": "error", 
            "error": str(e)
        })
    
    total_duration = time.time() - start
    
    # ספירת הצלחות
    success_count = len([r for r in results if r["status"] == "success"])
    
    # זמן ישראלי לתוצאה
    israel_tz = pytz.timezone('Asia/Jerusalem')
    timestamp = datetime.datetime.now(israel_tz).strftime("%d/%m/%Y %H:%M:%S")
    
    return jsonify({
        "status": f"🎯 SYNC ALL COMPLETED! {success_count}/{len(results)} successful",
        "total_duration": f"{total_duration:.1f}s",
        "sync_results": results,
        "timestamp": timestamp
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
