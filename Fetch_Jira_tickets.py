import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import os


JIRA_BASE_URL = "https://shripadpote95.atlassian.net"
EMAIL = os.environ.get("EMAIL")
API_TOKEN=os.environ.get("API")
auth = HTTPBasicAuth(EMAIL, API_TOKEN)
url = f"{JIRA_BASE_URL}/rest/api/3/search/jql"
jql = 'project = dashboard and parent = DEV-4'

def get_conn():
    try:
        connection = mysql.connector.connect(
        host=os.environ["HOST"],           # or "localhost"
        port=4000,
        user=os.environ["DB_USER"],
        password=os.environ["PASSWORD"],
        database="test",   # optional – can connect without DB first
        connect_timeout=100,
       
    )

        if connection.is_connected():
            print("Successfully connected to MySQL")
            return connection

    except Error as e:
        print(f"Error connecting to MySQL: {e}")

    
def fetch_data():

    headers = {
        "Accept": "application/json"
    }
    
    query = {
        'jql': jql,  # change JQL if needed
        'maxResults': 100,       # max per request
        'fields': 'summary,status,assignee,created,labels,priority,component'  # fetch only required fields
    }
    
    response = requests.get(
        url,
        headers=headers,
        params=query,
        auth=auth
    )
    
    response.raise_for_status()  # stops if API call fails
    data = response.json()
    
    print(f"Total issues fetched: {len(data.get('issues', []))}")
    
    rows=[]
    for issue in data.get('issues', []):
            fields = issue.get('fields', {})

            assignee = fields['assignee']['displayName'] if fields.get('assignee') else 'Unassigned'
            labels = fields.get("labels", [])
            priority = (
            fields.get("priority", {}).get("name")
            if fields.get("priority")
            else "None"
                )
            summary = fields.get('summary')
            
            rows.append({
                'ticket_no': issue['key'],
                'id': issue['id'],
                'module': summary.split('-')[1],
                'status': issue['fields'].get('status', {}).get('name', ''),
                'assignee': assignee,
                'created': issue['fields'].get('created', ''),
                'label' : ",".join(labels) if labels else "",
                'priority': priority

            })
    return pd.DataFrame(rows)

# ==============================
# INIT MAIN TABLE
# ==============================

def init_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS TICKET_STATUS_TIME (
        ticket_no VARCHAR(50) PRIMARY KEY,
        module VARCHAR(255),
        priority VARCHAR(50),
        label VARCHAR(255),
        open_time INT DEFAULT 0,
        in_analysis_time INT DEFAULT 0,
        ready_for_testing_time INT DEFAULT 0,
        reopened_count INT DEFAULT 0,
        current_status VARCHAR(50),
        assignee VARCHAR(255),
        last_updated TIMESTAMP
    )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TEMP_TICKET_STATUS_TIME (
        ticket_no VARCHAR(50) PRIMARY KEY,
        module VARCHAR(255),
        status VARCHAR(50),
        assignee VARCHAR(255),
        created TIMESTAMP,
        label VARCHAR(255),
        priority VARCHAR(50),
        last_updated TIMESTAMP
    )
    """)
    cur.execute("TRUNCATE TABLE TEMP_TICKET_STATUS_TIME")

def merge_data(conn, df):
    cur = conn.cursor()
    data = [
        tuple(row) for row in df[['ticket_no', 'module', 'status', 'assignee','created', 'label', 'priority']].itertuples(index=False)
    ]

    # === Bulk insert with executemany (fast) ===
    insert_query = """
    INSERT INTO TEMP_TICKET_STATUS_TIME (ticket_no, module, status, assignee,created, label, priority)
    VALUES (%s, %s, %s, %s,%s,%s,%s)
    """
    cur.executemany(insert_query, data)
    conn.commit()
    cur.execute(
    """
INSERT INTO TICKET_STATUS_TIME (
    ticket_no, module, priority, label,
    open_time, in_analysis_time, ready_for_testing_time,
    reopened_count, current_status, assignee, last_updated
)
SELECT
    ticket_no,
    module,
    priority,
    label,
    CASE WHEN UPPER(status) = 'OPEN' THEN 15 ELSE 0 END              ,
    CASE WHEN UPPER(status) = 'IN ANALYSIS' THEN 15 ELSE 0 END      ,
    CASE WHEN UPPER(status) = 'READY FOR TESTING' THEN 15 ELSE 0 END ,
    CASE WHEN UPPER(status) = 'REOPENED' THEN 1 ELSE 0 END           ,
    status         ,
    assignee,
    NOW()                             
FROM TEMP_TICKET_STATUS_TIME a
ON DUPLICATE KEY UPDATE
    
    open_time = open_time + CASE WHEN UPPER(status) = 'OPEN' THEN 15 ELSE 0 END,
    in_analysis_time = in_analysis_time + CASE WHEN UPPER(status) = 'IN ANALYSIS' THEN 15 ELSE 0 END,
    ready_for_testing_time = ready_for_testing_time + CASE WHEN UPPER(status) = 'READY FOR TESTING' THEN 15 ELSE 0 END,

    reopened_count = reopened_count + CASE WHEN UPPER(status) = 'REOPENED' THEN 1 ELSE 0 END,
    
    -- Always update these fields with new values
    current_status = a.status,
    priority       = a.priority,
    label          = a.label,
    assignee       = a.assignee,
    last_updated   = NOW()

    """
    )
    conn.commit()
    
# ==============================
# GENERATE OUTPUTS - TiDB compliant
# ==============================


def load_lookup_tables(conn):
    cur = conn.cursor()

    cur.execute("DROP  TABLE IF EXISTS SPOC")
    cur.execute("DROP  TABLE IF EXISTS SLA")

    cur.execute("""
    CREATE  TABLE SPOC (
        module VARCHAR(255),
        label VARCHAR(255),
        spoc VARCHAR(255)
    )
    """)

    cur.execute("""
    CREATE  TABLE SLA (
        status VARCHAR(50),
        allowed_time INT
    )
    """)

    pd.read_csv("SPOC.csv").pipe(
        lambda df: cur.executemany(
            "INSERT INTO SPOC VALUES (%s,%s,%s)", df.values.tolist()
        )
    )

    pd.read_csv("time_limit.csv").pipe(
        lambda df: cur.executemany(
            "INSERT INTO SLA VALUES (%s,%s)", df.values.tolist()
        )
    )

    conn.commit()





def main():

    conn = get_conn()
    cur = conn.cursor()
    df = fetch_data()
  
    
    init_db(cur)
    merge_data(conn, df)
    load_lookup_tables(conn)
    #generate_outputs(conn)

    conn.close()


main()
