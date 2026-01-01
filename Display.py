import streamlit as st
import pandas as pd
from PIL import Image
import re

@st.cache
def create_links_in_cell(cell_data):
    if isinstance(cell_data, str):
        # Find ticket numbers in the cell data and create clickable links
        ticket_numbers = re.findall(r'[A-Z]+-\d+', cell_data)
        links = [f'<a href="https://Jira.url/{ticket}" target="_blank">{ticket}</a>' for ticket in ticket_numbers]
        return ', '.join(links)
    else:
        return '-'

def main():
    # Load the data once at the start
    df1 = pd.read_excel('Dashboard.xlsx', sheet_name='Sheet1')
    df = pd.read_excel('Dashboard.xlsx', sheet_name='Sheet2')

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

    logo1 = Image.open('logo.png')
    st.image(logo1, use_column_width=True)

    st.subheader('Time in status')

    # Initialize session state for priority if not already set
    if 'selected_priority' not in st.session_state:
        st.session_state.selected_priority = df['priority'].unique().tolist()[0]

    # Priority selection
    unique_priority = sorted(df['priority'].unique().tolist())
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
    filtered_df = df[df['priority'] == st.session_state.selected_priority]
    filtered_df1 = df1[df1['Priority'] == st.session_state.selected_priority]

    with st.spinner('Loading data...'):
        custom_css = """
            <style>
                #custom-table {
                    width: 100%;
                    border-collapse: collapse;
                }
                #custom-table th, td {
                    padding: 8px;
                    text-align: left;
                    border: 1px solid #000;
                }
                #custom-table th {
                    background-color: #B0E0E6;
                    color: #000;  /* Dark black font color */
                    border-bottom: 1px solid #000;
                }
                #custom-table td {
                    background-color: #f9f9f9;
                    color: #333;
                    border: 1px solid #ddd;
                }
                #custom-table td:nth-child(5) {
                    color: #3CB371;  /* Dark green font for column 5 */
                    font-weight: bold;  /* Bold font for column 5 */
                }
                #custom-table td:nth-child(6) {
                    color: red;  /* Red font for column 6 */
                    font-weight: bold;  /* Bold font for column 6 */
                }
                .highlight {
                    background-color: #ff9999;  /* Darker Light Red */
                    color: #000;  /* Black font color */
                }
            </style>
        """
        st.write(f"<b>Details for [{st.session_state.selected_priority}] priority</b>", unsafe_allow_html=True)
        st.markdown(custom_css, unsafe_allow_html=True)

        # Display Table 1 with SPOC buttons
        st.write("### Table 1 with SPOC Selection:")

        formatted_df1 = filtered_df1.copy()

        # Define table headers to align content
        st.markdown(
            """
            <style>
                .row-header {
                    font-weight: bold;
                }
            </style>
            """, unsafe_allow_html=True
        )
        # Table headers for each column
        st.write(f"<div class='row-header'>View &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Priority &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Component &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Open Time (min) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; In Progress Time (min) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Resolved Time (min) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Reopened Count &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Current Status &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Assignee</div>", unsafe_allow_html=True)

        for index, row in formatted_df1.iterrows():
            col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns([1, 2, 2, 2, 2, 2, 2, 2, 2])  # Adjust column widths

            with col1:
                # Add a "View" button for each row
                if st.button("View", key=f"view_{index}"):
                    st.session_state.selected_spoc = row['SPOC']

            with col2:
                st.write(row['Priority'])

            with col3:
                st.write(row['Component'])

            with col4:
                st.write(row['Open Time (min)'])

            with col5:
                st.write(row['In Progress Time (min)'])

            with col6:
                st.write(row['Resolved Time (min)'])

            with col7:
                st.write(row['Reopened Count'])

            with col8:
                st.write(row['Current Status'])

            with col9:
                st.write(row['Assignee'])

        st.write('')
        st.write('')

        # Add a filter for tickets based on SPOC
        if 'selected_spoc' in st.session_state:
            st.write(f"Selected SPOC: {st.session_state.selected_spoc}")
            
            # Filter tickets for the selected SPOC
            spoc_filtered_df = filtered_df[filtered_df['SPOC'] == st.session_state.selected_spoc]

            # Display Table 2 (filtered tickets)
            custom_css1 = """
            <style>
                #custom-table2 {
                    width: 100%;
                    border-collapse: collapse;
                }
                #custom-table2 th, td {
                    padding: 8px;
                    text-align: left;
                    border: 1px solid #000;
                }
                #custom-table2 th {
                    background-color:#E6E6FA;
                    color: #000;  /* Dark black font color */
                    border-bottom: 1px solid #000;
                }
                #custom-table2 td {
                    background-color: #f9f9f9;
                    color: #333;
                    border: 1px solid #ddd;
                }
                .highlight {
                    background-color: #ff9999;  /* Darker Light Red */
                    color: #000;  /* Black font color */
                }
            </style>
            """
            st.markdown(custom_css1, unsafe_allow_html=True)

            st.write(f"<b>Tickets where [{st.session_state.selected_spoc}] is SPOC:</b>", unsafe_allow_html=True)

            filtered_df_1 = spoc_filtered_df.iloc[:, 0:1]
            formatted_df_with_links = filtered_df_1.applymap(create_links_in_cell)
            formatted_df_with_links.insert(1, 'Component', spoc_filtered_df['component'])
            formatted_df_with_links.insert(2, 'Open Time (in minutes)', spoc_filtered_df['open_time'])
            formatted_df_with_links.insert(3, 'In Progress Time (in minutes)', spoc_filtered_df['in_progress_time'])
            formatted_df_with_links.insert(4, 'Resolved Time (in minutes)', spoc_filtered_df['resolved_time'])
            formatted_df_with_links.insert(5, 'Reopened count', spoc_filtered_df['reopened_count'])
            formatted_df_with_links.insert(6, 'Current status', spoc_filtered_df['current_status'])
            formatted_df_with_links.insert(7, 'Assignee', spoc_filtered_df['asignee'])

            formatted_html = formatted_df_with_links.to_html(escape=False, index=False, table_id='custom-table2')
            st.markdown(formatted_html, unsafe_allow_html=True)

if __name__ == '__main__':
    main()

