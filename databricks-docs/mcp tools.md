# Connecting Agents to Code with MCP Tools

A workflow that brings data to Coding Agents and Desktop Apps.

```mermaid
--- 
title: Unity Catalog
---
flowchart TB
users & agents --> permissions -.-> data & functions

```


```mermaid
---
title: Govern MCP with Unity Gateway
---

flowchart TB
service_policy -- mcp_tools
~~~ mcp_servers --> custom_agents
~~~ connect_mcps --> coding_agents & ai_assistants
```


```mermaid
---
title: Connect a client
---
flowchart TB
Claude & Claude_code & ChatGPT & Cursor -- MCP_client --> MCP_SERVICE_URL --> authenticate
 --> databricks_data_and_code & databricks_hosted_services & custom_mcp_server
```


```mermaid
--- 
title: Genie One MCP Apps
---
flowchart LR
MCP_client & Agent 
-- Question --> 
Genie -- search --> data 
Genie -- write --> SQL
data & SQL --> interactive_view
-- Answer --> MCP_client & Agent 

```


```mermaid
--- 
title: Go to a previous conversation
---
flowchart TB
conversation_id & warehouse_id -.-> genie_ask --> conversation_id & response_id & status
--> genie_poll_response --> progress_steps & narration_instruction
status --> Visualizations

```

**Other native Genie tools**

- genie_get_query_result
- genie_cancel_response
- view_ask --> interactive_view



```mermaid
---
title: Genie Ontology - Semantic Layer
---
flowchart TB
business_terms
~~~ metric_definitions
~~~ table_relationships

```


```

## Unity Catalog Functions

You can store code functions in the Unity Catalog and have MCP clients access these resources. This is particularly useful for structure data retrieval from objects, such as Tableau workbooks.

```python
"""
To run this code:

1. Install libraries: https://docs.databricks.com/aws/en/libraries/workspace-files-libraries

2. Enable serverless compute: https://docs.databricks.com/gcp/en/compute/serverless/#requirements

"""

URL_Pattern = https://<workspace-hostname>/api/2.0/mcp/functions/{catalog}/{schema}/{function_name}

OAuth_scope =https://<workspace-hostname>/api/2.0/mcp/functions/{catalog}/{schema}/{function_name}
```



## MCP Servers

Ideal for undefined logic executed directly in agent code.

```mermaid
---
title: project workflow
---
flowchart TB

extract --> score & semantic_models

score -- meet_threshold? --> job_or_skill_recreate --> databricks_dashboard

semantic_models --> claude_connector --> q_&_a
```

