# Serverless compute
runtime
- Python 3.10.12
- databricks-connect 14.3.4

## Serverless client images
- A version grouping to wrap compatible python dependencies used for notebook and job
- Configure in notebook **Environment** panel > **Client version**
- version named like a sequence (start with `1`)


## Limit
- > GLOBAL TEMPORARY VIEW is not supported on serverless compute
- ?Once it was terminated and has not been inactive for a while . It cannot be recreated in current workspace
