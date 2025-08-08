# [Best practices for using](https://docs.databricks.com/aws/en/genie/conversation-api#-best-practices-for-using-the-conversation-api)

- The API does not manage request retries
  - **Implement request queuing and backoff**
  - **Use exponential backoff after 2 minutes**
- **Poll for status updates every 5 to 10 seconds**
  - until a conclusive message status, such as `COMPLETED`, `FAILED`, or `CANCELLED`, is received
  - polling timeout: 10 minutes
- **Start a new conversation for each session**
  - Avoid reusing conversation threads across sessions, as this can reduce accuracy due to unintended context reuse
- **Maintain conversation limits**
  - Use the [Delete conversation endpoint] to programatically delete conversations when a space approaches the 10,000 conversation limit


# Quota
Throughput limit
- **Unlimited**: during Public Preview period, the throughput rates are best-effort and depend on system capacity.
- Requests are limited to 5 queries per minute per workspace.
