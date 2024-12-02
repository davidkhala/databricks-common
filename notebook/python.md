
*python* in the context means a **raw file** with `.py` as suffix
- notebook name with `.py` as suffix is not considered a *python* here

# Case: notebook import notebook
[Run a Databricks notebook from another notebook](https://docs.databricks.com/en/notebooks/notebook-workflows.html)

## `%run` command
The simplest way to reuse
- Used for load the functions and variables in the called notebook into current notebook
- When you use `%run`, the called notebook is immediately executed.
- In order to pass parameters
    - [To Widget values in called notebook](https://docs.databricks.com/en/notebooks/widgets.html#use-databricks-widgets-with-run)
    - Furthermore, to get return values, Go to [dbutils.notebook.run()](#dbutilsnotebookrun)


Limit
- `%run` call to a Python file is not supported
    - [notebook import python](#case-notebook-import-python)
- > the `%run` command needs to be in a cell **all by itself**
  - It occupies entire notebook cell. no other code and no comments
  - Otherwise you can see error like
  ```
      /databricks/python/lib/python3.10/site-packages/IPython/core/interactiveshell.py:2855: UserWarning: Could not open file </Workspace/Shared/notebook_called> for safe execution.
     warn('Could not open file <%s> for safe execution.' % fname)
  ```
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
It is supported. To run python directly, use either
- `%sh` in notebook
- Databricks **Web terminal**

# Case: Python import notebook
load a notebook is not supported
- If `./azure_open_datasets/index` is notebook (not a python file), loading it by `import azure_open_datasets.index` is not supported 
- It is allowed only when it is `./azure_open_datasets/index.py`, as a python file only

# Case: notebook import Python
> [You can import a file into a notebook using standard Python import commands](https://docs.databricks.com/en/notebooks/share-code.html)
> **Tip: use autoreload when you're modifying Python modules.**
_Enable autoreload_ to pick up changes to modules without restarting Python.

## `dbutils.import_notebook(...)`
- unofficial API
- This is a wrapper of `importlib.import_module(path)`. Similar to standard Python import with programmable module name
- Cannot be used for notebook importing notebook