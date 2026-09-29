from databricks.sdk import WorkspaceClient
import time

# 1. Initialize client (Automatically picks up DATABRICKS_HOST and DATABRICKS_TOKEN from env variables)
w = WorkspaceClient()
space_id = "YOUR_GENIE_SPACE_ID_HERE" # (e.g., a 32-character hex string)
question = """
You are recreating a single Tableau dashboard as an AI/BI (Lakeview) dashboard in Databricks.


CRITICAL CONSTRAINTS:
You must ONLY recreate the dashboard from the Tableau workbook at: {{workbook_path}}
You must ONLY use these exact Unity Catalog tables: {{extracted_tables}}
Do NOT search for, discover, or use any other tables in the workspace even if they exist in the same schema or catalog. The tables listed above are the complete and only data source for this dashboard.
Do NOT create dashboards for any other workbooks. Your scope is strictly the single workbook at {{workbook_path}}.
Your task:
1. Parse the comma-separated list in {{extracted_tables}} to identify each fully qualified table name.
2. For each table, inspect its schema (columns, types) to understand the available data.
3. Create a single new AI/BI dashboard. Name it after the workbook filename derived from {{workbook_path}} (e.g. if the path ends in "SuperStore Test Mid.twbx", name the dashboard "SuperStore Test Mid").
4. For each of the provided tables, create appropriate visualizations:
   - Use counters/KPIs for single aggregate metrics
   - Use bar charts for categorical comparisons
   - Use line charts for time-series data
   - Use tables for detailed row-level data
   - Add filters where dimension columns suggest user interactivity
5. Organize widgets across dashboard pages logically — group related visualizations together.
6. If any of the listed tables has no rows or the schema is empty, note it in your summary but do not fail.
7. After building, verify that each widget renders correctly. Fix any SQL or rendering errors before finishing.
8. Publish the dashboard when complete.


On error or empty results: Report which tables could not be visualized and why, rather than silently skipping them.
"""

# 2. Ask the question to Genie
message = w.genie.start_conversation(
    space_id=space_id,
    content=question
)
conversation_id = message.conversation_id
message_id = message.id

print(f"Asked Genie! Conversation ID: {conversation_id}")

# 3. Poll until Genie is done thinking
while True:
    response = w.genie.get_message(
        space_id=space_id, 
        conversation_id=conversation_id, 
        message_id=message_id
    )
    
    if response.status.value == "COMPLETED":
        print("\nGenie Response:")
        print(response.content)
        break
    elif response.status.value in ["FAILED", "CANCELED"]:
        print(f"\nQuery stopped with status: {response.status.value}")
        break
        
    print("Waiting for Genie to finish...")
    time.sleep(3) # Wait 3 seconds before checking again