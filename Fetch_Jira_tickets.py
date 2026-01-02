

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import duckdb
from datetime import datetime

# --------------- CONFIG ---------------
JIRA_BASE_URL = "https://shripadpote95.atlassian.net"

EMAIL = "shripadpote95@gmail.com"
API_TOKEN = "ATATT3xFfGF0Mn5amQImj2JuXZl52KcilJQWMeuD2noRvKnnhljDMLsVtVCYeKtoyTW4m6hSgiHe19HDt2Un1U7gqVkj34p2HwkFtHzBJXs54pwnhUUs5PDSLuhq6FJAsAhoBK_vcK--IHCIoIto-Av14Qnuz11KiSMVskqKeOi0cnc2HtROO6U=C87D5C11"


auth = HTTPBasicAuth(EMAIL, API_TOKEN)
url = f"{JIRA_BASE_URL}/rest/api/3/search/jql"
jql = 'project = dashboard and parent = DEV-4'
DB_FILE = "jira_sla.duckdb"

def fetch_data():

    headers = {
        "Accept": "application/json"
    }
    
    query = {
        'jql': jql,  # change JQL if needed
        'maxResults': 100,       # max per request
        'fields': 'summary,status,assignee,created,labels,priority'  # fetch only required fields
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
            
            rows.append({
                'ticket_no': issue['key'],
                'id': issue['id'],
                'summary': issue['fields'].get('summary', ''),
                'status': issue['fields'].get('status', {}).get('name', ''),
                'assignee': assignee,
                'created': issue['fields'].get('created', ''),
                'label' : ",".join(labels) if labels else "",
                'priority': priority

            })
    return pd.DataFrame(rows)

def init_db(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS TICKET_STATUS_TIME (
        ticket_no VARCHAR PRIMARY KEY,
        priority VARCHAR,
        label VARCHAR,
        open_time INTEGER DEFAULT 0,
        in_progress_time INTEGER DEFAULT 0,
        resolved_time INTEGER DEFAULT 0,
        reopened_count INTEGER DEFAULT 0,
        current_status VARCHAR,
        assignee VARCHAR,
        last_updated TIMESTAMP
    )
    """)

def merge_data(con, df):
    con.register("df_tmp", df)

    con.execute("""
    CREATE OR REPLACE TEMP TABLE TEMP_TICKET_STATUS_TIME AS
    SELECT
        ticket_no,
        LOWER(REPLACE(status, ' ', '_')) AS status,
        priority,
        label,
        assignee
    FROM df_tmp
    """)

    con.execute("""
    MERGE INTO TICKET_STATUS_TIME mst
    USING TEMP_TICKET_STATUS_TIME tmp
    ON mst.ticket_no = tmp.ticket_no

    WHEN MATCHED THEN
    UPDATE SET
        open_time = mst.open_time
            + CASE WHEN tmp.status = 'to_do' THEN 15 ELSE 0 END,

        in_progress_time = mst.in_progress_time
            + CASE WHEN tmp.status = 'in_progress' THEN 15 ELSE 0 END,

        resolved_time = mst.resolved_time
            + CASE WHEN tmp.status = 'resolved' THEN 15 ELSE 0 END,

        reopened_count = mst.reopened_count
            + CASE
                WHEN tmp.status = 'reopened'
                 AND mst.current_status <> 'reopened'
                THEN 1 ELSE 0
              END,

        current_status = tmp.status,
        priority = tmp.priority,
        label = tmp.label,
        assignee = tmp.assignee,
        last_updated = CURRENT_TIMESTAMP

    WHEN NOT MATCHED THEN
    INSERT (
        ticket_no,
        priority,
        label,
        open_time,
        in_progress_time,
        resolved_time,
        reopened_count,
        current_status,
        assignee,
        last_updated
    )
    VALUES (
        tmp.ticket_no,
        tmp.priority,
        tmp.label,
        CASE WHEN tmp.status = 'open' THEN 15 ELSE 0 END,
        CASE WHEN tmp.status = 'in_progress' THEN 15 ELSE 0 END,
        CASE WHEN tmp.status = 'resolved' THEN 15 ELSE 0 END,
        0,
        tmp.status,
        tmp.assignee,
        CURRENT_TIMESTAMP
    )
    """)

def main():
    con = duckdb.connect(DB_FILE)
    try:
        con.execute("BEGIN TRANSACTION")

        init_db(con)

        df = fetch_data()
        merge_data(con, df)

        con.execute("COMMIT")
        print("Merge successful at", datetime.now())
        df = con.execute("""
        SELECT * FROM TICKET_STATUS_TIME
        """).df()
        print(df)
        df.to_csv("dashboard.csv", index=False)

    except Exception as e:
        con.execute("ROLLBACK")
        print("Error:", e)

    finally:
        con.close()

if __name__ == '__main__':
    main()
