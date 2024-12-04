import requests
import subprocess

def send_sql_query(api_url: str, query_type: str, tables: str, sql_query: str, query_id: str) -> dict:
    try:
        # Prepare JSON payload
        payload = {
            "type": query_type,
            "tables": tables,
            "query": sql_query,
            "query_id": query_id
        }

        # Send POST request
        headers = {"Content-Type": "application/json"}
        response = requests.post(api_url, json=payload, headers=headers)
        
        # Check for response status
        response.raise_for_status()
        
        # Parse JSON response
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return {"error": str(e)}

# Example usage
api_url = "http://localhost:5001/send_task"
query_type = "ResponseType.QUERY"
tables = ["lineitem"]
query_id = "1"
# Read SQL query from file queries/1.sql
sql_query = open("test_queries/1.sql").read()

response = send_sql_query(api_url, query_type, tables, sql_query, query_id)
print(response)

# Long running: 2, 17, 20
# Queries that work: 1, 2, 3, 5, 6, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19
# Queries that don't work: 4, 13, 17, 20

# Use docker cp to retrieve the results from the container
# docker cp <container_id>:/app/query-results ./
subprocess.run(["docker", "cp", "aggregator:/app/query-results", "./"])