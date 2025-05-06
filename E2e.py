"""
oracle_connect.py

Connects to an Oracle Database using pyodbc and Oracle Instant Client
without requiring admin privileges.

Ensure you have:
- Downloaded and extracted Oracle Instant Client (Basic + ODBC) locally
- Updated your user PATH to include the extracted folder
- Checked the available ODBC drivers using pyodbc.drivers()
"""

import pyodbc

# Replace these values with your Oracle DB details
HOST = 'your_host'               # e.g., '127.0.0.1' or 'mydb.example.com'
PORT = '1521'                    # default port
SERVICE_NAME = 'your_service'    # e.g., 'orclpdb1'
USERNAME = 'your_user'
PASSWORD = 'your_password'

# Check available ODBC drivers (to find the correct Oracle driver name)
print("Available ODBC Drivers:")
print(pyodbc.drivers())

# NOTE: Update the driver name if it's different in your environment
DRIVER_NAME = 'Oracle in InstantClient'

# Build the DSN-less connection string
dsn = f"{HOST}:{PORT}/{SERVICE_NAME}"
conn_str = (
    f"Driver={{{DRIVER_NAME}}};"
    f"DBQ={dsn};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD}"
)

try:
    # Establish the connection
    conn = pyodbc.connect(conn_str)
    print("Connected to Oracle Database successfully.")

    # Create a cursor and execute a sample query
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM YOUR_TABLE_NAME")  # replace with your actual table

    # Fetch and print results
    for row in cursor.fetchall():
        print(row)

    # Clean up
    cursor.close()
    conn.close()

except Exception as e:
    print("Failed to connect or execute query:")
    print(e)
