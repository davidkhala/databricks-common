https://docs.databricks.com/aws/en/query-federation/http

pre-requisite

- External services must comply with OAuth 2.0 specifications. authentication should be one of:
  - Bearer token: Obtain a bearer token for simple token-based authentication.
  - OAuth 2.0 Machine-to-Machine: Create and configure an app to enable machine-to-machine authentication.
  - OAuth 2.0 User-to-Machine Shared: Authenticate with user interaction to share access between service identity and machine.
  - OAuth 2.0 User-to-Machine Per User: Authenticate with per user interaction to access between user identity and machine.

extension: [reuse http connections for agent tools](https://docs.databricks.com/aws/en/generative-ai/agent-framework/external-connection-tools)
