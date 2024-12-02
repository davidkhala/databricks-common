
*python* in the context means a **raw file** with `.py` as suffix
- notebook name with `.py` as suffix is not considered a *python* here

# Case: notebook import notebook
[Run a Databricks notebook from another notebook](https://docs.databricks.com/en/notebooks/notebook-workflows.html)

## `dbutils.import_notebook(...)`
> Importing notebooks directly (by `import azure_open_datasets.index`) is not supported. Use dbutils.import_notebook("azure_open_datasets.index") instead.
- No document page found
- Used for load the functions and variables in the called notebook into current notebook

## `%run` command
The simplest way to reuse
- same with [`dbutils.import_notebook`](#dbutilsimport_notebook)
- When you use `%run`, the called notebook is immediately executed.
- In order to pass parameters
    - [To Widget values in called notebook](https://docs.databricks.com/en/notebooks/widgets.html#use-databricks-widgets-with-run)
    - Furthermore, to get return values, Go to [dbutils.notebook.run()](#dbutilsnotebookrun)
- `%run` call to a Python file is not supported
    - [notebook import python](#case-notebook-import-python)


## dbutils.notebook.run()
- It will **start a new job** to run the notebook
- Not supported for R or SQL
- Both params and return value are strings only

`run(path: String,  timeout_seconds: int, arguments: Map): String`
- `timeout_seconds=0` for specify no timeout
- `arguments` sets widget values
- used in caller notebook

`dbutils.notebook.exit("returnValue")` to specify returning value
- used in called notebook

# Case: Python import Python
It is supported
- To run python directly. use `%sh` in notebook

# Case: Python import notebook
load a notebook is not supported
- If `./azure_open_datasets/index` is notebook (not a python file), loading it by `import azure_open_datasets.index` is not supported 
- It is allowed only when it is `./azure_open_datasets/index.py`, as a python file only

# Case: notebook import Python
> [You can import a file into a notebook using standard Python import commands](https://docs.databricks.com/en/notebooks/share-code.html)