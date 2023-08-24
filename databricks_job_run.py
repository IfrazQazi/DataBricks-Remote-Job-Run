import requests

# Define the API endpoint
url = "https://adb-7659051096892963.3.azuredatabricks.net/api/2.0/jobs/run-now"

"""
To Generate Authorization Token follow below steps:
1.Go to your Azure Databricks workspace.
2.Click on the user profile icon in the top right corner.
3.Select "User Settings."
4.Under the "Access Tokens" section, click on "Generate New Token."
5.Once you generate the access token, replace the "Bearer dapiaf125f0d97460211288ed456fd76b677" in the "Authorization" header with your generated access token.
"""
# Provide the required headers and access token

headers = {
    "Authorization": "Bearer dapiaf125f0d97460211288ed456fd76b677",
    "Content-Type": "application/json"
}

"""
 The notebook's ID, you can find it in the URL when you're viewing the notebook. 
 The URL will look like https://<databricks-instance>#notebook/<notebook_id>.
"""

notebook_path_or_id = "2740869648252537"  # Replace with the actual path or ID

job_id="628272968089511"
# Optional: Provide any additional parameters if required
params = {
    "run_name": "My Notebook Run",
    "notebook_task": {
        "notebook_path": notebook_path_or_id
    },
    "job_id":job_id
}
# Send the POST request to run the entire notebook
response = requests.post(url, headers=headers, json=params)

# Check the response status code to ensure the job was submitted successfully
if response.status_code == 200:
    print("Notebook submitted successfully.")
else:
    print(f"Error: {response.text}")
