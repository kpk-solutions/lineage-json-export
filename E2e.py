import cx_Oracle

def copy_table(dev_table_name, qa_table_name, qa_user, qa_pass, qa_host, qa_service):
    try:
        # Connect to QA DB
        dsn = cx_Oracle.makedsn(qa_host, 1521, service_name=qa_service)
        conn = cx_Oracle.connect(user=qa_user, password=qa_pass, dsn=dsn)
        cursor = conn.cursor()

        # Source table from DEV using DB link
        source_table = f"{dev_table_name}@dev_link"

        print(f"Fetching data from {source_table}...")
        cursor.execute(f"SELECT * FROM {source_table}")
        rows = cursor.fetchall()

        if not rows:
            print("No data found in source table.")
            return

        # Get column names
        columns = [col[0] for col in cursor.description]
        col_str = ', '.join(columns)
        placeholders = ', '.join([f":{i+1}" for i in range(len(columns))])

        # Prepare insert query for QA table
        insert_query = f"INSERT INTO {qa_table_name} ({col_str}) VALUES ({placeholders})"

        print(f"Inserting into {qa_table_name}...")
        cursor.executemany(insert_query, rows)
        conn.commit()
        print(f"Copied {len(rows)} rows from {source_table} to {qa_table_name}.")

    except cx_Oracle.DatabaseError as e:
        print("Database error occurred:", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # CONFIGURE THESE
    DEV_TABLE = "your_dev_table_name"     # Without @dev_link
    QA_TABLE = "your_qa_table_name"

    QA_USER = "qa_user"
    QA_PASS = "qa_password"
    QA_HOST = "qa_host_or_ip"             # e.g., 192.168.1.100
    QA_SERVICE = "qa_service_name"

    copy_table(DEV_TABLE, QA_TABLE, QA_USER, QA_PASS, QA_HOST, QA_SERVICE)
