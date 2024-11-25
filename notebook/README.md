> Formatting cells with language Scala is unsupported.

# Databricks notebook authentication
> Default Databricks notebook authentication works only on the cluster’s driver node
- not supported in cluster’s worker or executor nodes.

# Import another notebook
[Run a Databricks notebook from another notebook](https://docs.databricks.com/en/notebooks/notebook-workflows.html)

## `%run` command
The simplest way to reuse
- Similar to `#include`, `import * from ...`/`require()` in JS
- When you use %run, the called notebook is immediately executed.
- It will not starts a new job
- In order to pass parameters
    - [To Widget values in called notebook](https://docs.databricks.com/en/notebooks/widgets.html#use-databricks-widgets-with-run)
    - Furthermore, to get return values, Go to [dbutils.notebook.run()](#dbutilsnotebookrun)
- Used for load the functions and variables in the called notebook into current notebook
- `%run` call to a Python file is not supported
    - > [You can import a file into a notebook using standard Python import commands](https://docs.databricks.com/en/notebooks/share-code.html)


## dbutils.notebook.run()
- It will starts a new job to run the notebook
- Not supported for R or SQL
- Both params and return value are strings only

`run(path: String,  timeout_seconds: int, arguments: Map): String`
- `timeout_seconds=0` for specify no timeout
- `arguments` sets widget values
- used in caller notebook

`dbutils.notebook.exit("returnValue")` to specify returning value
- used in called notebook