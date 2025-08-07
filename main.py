def main(request):
    import psycopg2
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    # חיבור ל־PostgreSQL
    conn = psycopg2.connect(
        host='rtngplsadmin40.data-driven.media',
        port=5432,
        dbname='clients_managment',
        user='looker_mediaforest',
        password='<< הסיסמה שלך כאן >>'
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM campaign_summary_last_7_days_new")
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    # חיבור ל־Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open("<< שם הגיליון >>").worksheet("<< שם הטאב >>")
    sheet.clear()
    sheet.insert_row(headers, 2)
    sheet.insert_rows(rows, 3)

    return 'Success'

