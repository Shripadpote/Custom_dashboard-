import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import csv

#  function to scrape a page
def scrape_page(url):
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    print(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr', id=lambda x: x and x.startswith('issuerow'))
        for row in rows:
            issue_key_elem = row.find('td', class_='issuekey').find('a', class_='issue-link')
            issue_key = issue_key_elem['data-issue-key'].strip() if issue_key_elem else "N/A"

            assignee_elem = row.find('td', class_='assignee').find('a', class_='user-hover-replaced')
            assignee = assignee_elem.text.strip() if assignee_elem else "N/A"

            status_elem = row.find('td', class_='status').find('span')
            status = status_elem.text.strip() if status_elem else "N/A"

            created_date_elem = row.find('td', class_='created').find('time')
            created_date = created_date_elem.text.strip() if created_date_elem else "N/A"

            components_elem = row.find('td', class_='components')
            if components_elem:
                components = [component.strip() for component in components_elem.text.strip().split(',')]
            else:
                components = []

            issue_data = {
                'Issue Key': issue_key,
                'Assignee': assignee,
                'Status': status,
                'Created Date': created_date
            }

            for i, component in enumerate(components):
                issue_data[f'Component {i + 1}'] = component

            data.append(issue_data)
        
    else:
        print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")

# Initialize variables
data = []
count = 0

response = requests.get(jira_url, auth=HTTPBasicAuth(username, password))
soup = BeautifulSoup(response.text, 'html.parser')

total_count = soup.find('span', class_='results-count-total results-count-link').text.strip()
print(total_count)

while count < int(total_count):
    url = jira_url + "&startIndex=" + str(count)
    scrape_page(url)
    count += 50

# Determine the maximum number of components to define the headers
max_components = max(len([key for key in item.keys() if key.startswith('Component')]) for item in data) if data else 0
headers = ['Issue Key', 'Assignee', 'Status', 'Created Date'] + [f'Component {i + 1}' for i in range(max_components)]

# Write to CSV
with open('jira_issues.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    writer.writerows(data)

print("Data has been written to jira_issues.csv")

