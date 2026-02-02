import streamlit as st
import pandas as pd
import re
import altair as alt
import time
import mysql.connector
import os

def get_conn():

        connection = mysql.connector.connect(
        host=os.environ.get("host"),           # or "localhost"
        port=int(os.environ.get("port")),
        user=os.environ.get("user"),
        password=os.environ.get("password"),
        database=os.environ.get("database"),   # optional – can connect without DB first
        connect_timeout=10,
       
    )

        if connection.is_connected():
            print("Successfully connected to MySQL")
            return connection

    

@st.cache_data 
def create_links_in_cell(cell_data):
    if isinstance(cell_data, str):
        # Find ticket numbers in the cell data and create clickable links
        ticket_numbers = re.findall(r'[A-Z]+-\d+', cell_data)
        links = [f'<a href="https://Jira.url/{ticket}" target="_blank">{ticket}</a>' for ticket in ticket_numbers]
        return ', '.join(links)
    else:
        return '-'

def main():
    con = get_conn()
    df1 = pd.read_sql("""
    SELECT
        a.*,
        s.spoc  as SPOC,
        CASE
            WHEN upper(a.current_status)='OPEN' AND a.open_time > l.allowed_time THEN 'Needs attention'
            WHEN upper(a.current_status)='IN_ANALYSIS' AND a.in_analysis_time > l.allowed_time THEN 'Needs attention'
            WHEN upper(a.current_status)='READY_FOR_TESTING' AND a.ready_for_testing_time > l.allowed_time THEN 'Needs attention'
            ELSE 'Within limit'
        END AS verdict
    FROM TICKET_STATUS_TIME a
    JOIN SLA l ON upper(a.current_status) = upper(l.status)
    JOIN SPOC s ON a.module = s.module AND a.label = s.label
    """, con)



    df = pd.read_sql(""" 
    SELECT
        priority,spoc as SPOC, module, label,
        SUM(CASE WHEN verdict='Needs attention' THEN 1 ELSE 0 END) AS NEED_ATTENTION,
        SUM(CASE WHEN verdict='Within limit' THEN 1 ELSE 0 END) AS Within_limit
    FROM (
        SELECT
            a.priority,s.spoc, a.module, a.label,
             CASE
            WHEN upper(a.current_status)='OPEN' AND a.open_time > l.allowed_time THEN 'Needs attention'
            WHEN upper(a.current_status)='IN_ANALYSIS' AND a.in_analysis_time > l.allowed_time THEN 'Needs attention'
            WHEN upper(a.current_status)='READY_FOR_TESTING' AND a.ready_for_testing_time > l.allowed_time THEN 'Needs attention'
            ELSE 'Within limit'
            END AS verdict
        FROM TICKET_STATUS_TIME a
        JOIN SLA l ON upper(a.current_status) = upper(l.status)
        JOIN SPOC s ON a.module = s.module AND a.label = s.label
    ) t
    GROUP BY priority, spoc, module, label
    """, con)


    con.close()
    df = df.sort_values(
    by=["NEED_ATTENTION","SPOC"],
    ascending=[False,True]
        )
    # Load the data once at the start
   # df1 = pd.read_csv('dashboard.csv')
    #df = pd.read_csv('grouped.csv')

    st.set_page_config(page_title='Time in status', layout='wide', initial_sidebar_state='expanded')

    # Remove Streamlit footer
    hide_st_style = """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>
        """
    st.markdown(hide_st_style, unsafe_allow_html=True)

    text1 = "For the best view, please use the light theme for your web browser"
    st.markdown(f"<p style='font-size: 10px;'>{text1}</p>", unsafe_allow_html=True)

    #logo1 = Image.open('logo.png')
    #st.image(logo1, use_column_width=True)

    st.subheader('Time in status')

    # Initialize session state for priority if not already set
    if 'selected_priority' not in st.session_state:
        st.session_state.selected_priority = df1['priority'].unique().tolist()[0]
    if 'selected' not in st.session_state:
                st.session_state.selected = df.iloc[0,1]

    # Priority selection
    unique_priority = sorted(df1['priority'].unique().tolist())
    selected_priority = st.radio(
        'Select Priority *',
        unique_priority,
        index=False,
        horizontal=True
    )

    # Update session state if the selection changes
    if selected_priority != st.session_state.selected_priority:
        st.session_state.selected_priority = selected_priority

    # Filter data based on the selected priority
    #filtered_df = df[df['priority'] == st.session_state.selected_priority]
    filtered_df1 = df1[df1['priority'] == st.session_state.selected_priority]
    df= df[df['priority'] == st.session_state.selected_priority]
    with st.spinner('Loading data...'):
        styled_html = f"""
<style>
.mystyle {{
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
}}

.mystyle th, .mystyle td {{
    border: 1px solid #ddd;
    padding: 6px 8px;
    text-align: center;
}}

.mystyle thead th {{
    background-color: #87CEEB;
    color: #000;
    position: sticky;
    top: 0;
    z-index: 10;
}}

.mystyle tr:nth-child(even) {{
    background-color: #f9f9f9;
}}

.mystyle tr:hover {{
    background-color: #e6f2ff;
}}

.scroll-container {{
    max-height: 600px;
    overflow: auto;
    border: 1px solid #ccc;
    border-radius: 8px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
}}
</style>
"""

        st.write(f"<b>Details for [{st.session_state.selected_priority}] priority</b>", unsafe_allow_html=True)
        st.markdown(styled_html, unsafe_allow_html=True)
        col1,col2=st.columns(2)
        
        with col1:
            st.write("Select a SPOC to check tickets which needs attention" )
            df=df.iloc[:,1:]
            selected = st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            selection_mode="single-row",
            on_select="rerun"
            )
            
            
        with col2:
            SPOC_df=df[["SPOC","NEED_ATTENTION"]]
            bars = alt.Chart(SPOC_df).mark_bar(color='red').encode(
            x=alt.X('SPOC:N', title='SPOC'),
            y=alt.Y('NEED_ATTENTION:Q', title='NEED_ATTENTION', stack='zero'),
            tooltip=['SPOC', 'NEED_ATTENTION']
            )

            text_inside = alt.Chart(SPOC_df).mark_text(
                dy=6,
                fontSize=12,
                fontWeight='bold',
                color='white'
            ).encode(
                x='SPOC:N',
                y=alt.Y('NEED_ATTENTION:Q', stack='zero'),
                text='NEED_ATTENTION:Q',
            )

            chart = bars + text_inside
            st.altair_chart(chart, use_container_width=True)
        
        if selected and selected.selection and selected.selection.rows:
                row_pos = selected.selection.rows[0]
                new_selected = df.iloc[row_pos]
                new_SPOC=new_selected["SPOC"]

            # Only update if actually different
                if not new_SPOC.equals(st.session_state.selected):
                        st.session_state.selected = new_SPOC
        
        selected_value = st.session_state.selected
        if selected_value:
                st.write("Tickets for SPOC which needs attention: ", selected_value)
        #st.dataframe(df,hide_index=True)
                st.write('')
            
                formatted_df1 = filtered_df1.copy()
                formatted_df1=formatted_df1[formatted_df1["SPOC"] == selected_value]
                formatted_df1=formatted_df1[formatted_df1["verdict"] == 'Needs attention']
    
        #filtered_df = df[df["DOMAIN"] == "Billing"]
                formatted_df1["ticket_no"]=formatted_df1["ticket_no"].apply(
           lambda x : f'<a href="https://shripadpote95.atlassian.net/browse/{x}" target="_blank">{x}</a>'
            )
      
                html_table= formatted_df1.to_html(
            index=False,
            justify="center",
            border=0,
            escape=False,
            classes="mystyle"
            )
                st.markdown(styled_html+f'<div class="scroll-container">{html_table}</div>',unsafe_allow_html=True)



if __name__ == '__main__':
    main()
