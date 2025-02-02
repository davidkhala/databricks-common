from databricks.sdk.runtime import sc

# properties
assert sc.version == '3.5.2'
assert sc.pythonVer == '3.12'
assert sc.sparkHome == '/databricks/spark'
assert sc.appName == 'Databricks Shell'
print(sc.master)  # 'local[*, 4]' for personal compute Standard_DS3_v2

assert sc.environment['PYTHONHASHSEED'] == '0'
assert sc.pythonExec == '/databricks/python/bin/python'

assert sc.uiWebUrl.endswith('40001') and sc.uiWebUrl.startswith('http://')
