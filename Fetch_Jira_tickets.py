import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import duckdb
from datetime import datetime

# --------------- CONFIG ---------------
JIRA_BASE_URL = "https://shripadpote95.atlassian.net"
#---Sample credentials
EMAIL = "shripad****@gmail.com"
API_TOKEN=""
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

def init_db(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS TICKET_STATUS_TIME (
        ticket_no VARCHAR PRIMARY KEY,
        module VARCHAR,
        priority VARCHAR,
        label VARCHAR,
        open_time INTEGER DEFAULT 0,
        in_analysis_time INTEGER DEFAULT 0,
        ready_for_testing_time INTEGER DEFAULT 0,
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
        module,
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
            + CASE WHEN upper(tmp.status) = 'OPEN' THEN 15 ELSE 0 END,

        in_analysis_time = mst.in_analysis_time
            + CASE WHEN upper(tmp.status) = 'IN_ANALYSIS' THEN 15 ELSE 0 END,

        ready_for_testing_time = mst.ready_for_testing_time
            + CASE WHEN upper(tmp.status) = 'READY_FOR_TESTING' THEN 15 ELSE 0 END,

        reopened_count = mst.reopened_count
            + CASE
                WHEN tmp.status = 'Reopened'
                 AND mst.current_status <> 'Reopened'
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
        module,
        priority,
        label,
        open_time,
        in_analysis_time,
        ready_for_testing_time,
        reopened_count,
        current_status,
        assignee,
        last_updated
    )
    VALUES (
        tmp.ticket_no,
        tmp.module,
        tmp.priority,
        tmp.label,
        CASE WHEN tmp.status = 'Open' THEN 15 ELSE 0 END,
        CASE WHEN tmp.status = 'In Analysis' THEN 15 ELSE 0 END,
        CASE WHEN tmp.status = 'Ready for Testing' THEN 15 ELSE 0 END,
        0,
        tmp.status,
        tmp.assignee,
        CURRENT_TIMESTAMP
    )
    """)
    df_spoc=pd.read_csv("SPOC.csv")
    df_sla=pd.read_csv("time_limit.csv")

    con.execute("""
    CREATE OR REPLACE TEMP TABLE SPOC AS
    SELECT
       module,label,SPOC
    FROM df_spoc
    """)

    con.execute("""
    CREATE OR REPLACE TEMP TABLE sla AS
    SELECT
       status, allowed_time,
    FROM df_sla
    """)

    con.execute("""
    CREATE OR REPLACE TEMP TABLE TEMP_TICKET_STATUS_FINAL AS
    SELECT
        a.*,
        c.spoc,
        case when upper(a.current_status)='OPEN' and a.open_time > b.allowed_time  then 'Needs attention'
         when upper(a.current_status)='IN_ANALYSIS' and a.open_time > b.allowed_time  then 'Needs attention'
         when upper(a.current_status)='READY_FOR_TESTING' and a.open_time > b.allowed_time  then 'Needs attention'  
        else 'Within limit' end as verdict     
        from 
        TICKET_STATUS_TIME a
                join sla b on a.current_status =b.status 
                join spoc c on a.module=c.module and a.label=c.label
    """)

    df = con.execute("""
        SELECT * FROM TEMP_TICKET_STATUS_FINAL
        """).df()
   
    df.to_csv("dashboard.csv", index=False)

    df1 = con.execute("""
         with temp as (             
        SELECT SPOC,module,label, case when verdict = 'Needs attention' then 1 else 0 end as NEED_ATTENTION,
                      case when verdict = 'Within limit' then 1 else 0 end as Within_limit
                       FROM TEMP_TICKET_STATUS_FINAL)
                      select SPOC,module,label,sum(NEED_ATTENTION) as NEED_ATTENTION ,sum(Within_limit) as Within_limit from temp
                      group by SPOC,module,label
        """).df()
    
    df1.to_csv("grouped.csv", index=False)


def main():
    con = duckdb.connect(DB_FILE)
    try:
        con.execute("BEGIN TRANSACTION")

        init_db(con)

        df = fetch_data()
        merge_data(con, df)

        con.execute("COMMIT")
        print("Merge successful at", datetime.now())
      

    except Exception as e:
        con.execute("ROLLBACK")
        print("Error:", e)

    finally:
        con.close()

if __name__ == '__main__':
    main()
