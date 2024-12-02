# [pip `databricks-connect`](https://pypi.org/project/databricks-connect/)
- `databricks-connect` is not open source.

prerequisite
- login already 
- Its dependency numpy `1.26.4` requires g++
## Inline test run
```shell
pip install pipx

pipx run databricks-connect test
```
## Global test run
It is not recommended
- it will overwrite your global `numpy` installation
```
pip install databricks-connect --break-system-packages
databricks-connect test
```


