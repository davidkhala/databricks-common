
# prototype an Agent
- You can add up to 20 tools

limit
- rigid rate limit on GPT-5.2
  - `Error code: 403 - {'error_code': 'PERMISSION_DENIED', 'message': 'PERMISSION_DENIED: The endpoint is temporarily disabled due to a Databricks-set rate limit of 0.'}`
## Tools

### Unity Catalog (UC) Function
- choose a function or a containing schema
### Vector Search
- choose a vector search index

### MCP Servers (beta)
Enable required
> To add Model Context Protocol (MCP) servers of your Unity Catalog Functions, Vector Search Indexes, Genie Spaces, External MCP severs or custom Databricks Apps MCP Servers, please enroll in the managed MCP servers beta.
- Under [workspace-level preview](https://learn.microsoft.com/en-us/azure/databricks/admin/workspace-settings/manage-previews#-manage-workspace-level-previews)
  - named as **Managed MCP Servers**

[azure doc](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/mcp/managed-mcp)

#### Databricks Managed MCP Servers
Unity Catalog Function
- specify multiple schemas
Vector Search
- specify multiple schemas
Genie Space

#### External MCP Servers
Unity Catalog Connection
- installed from marketplace

#### MCP Servers on Databricks Apps
Custom MCP Server
- Only apps whose names start with 'mcp-' are shown here.
