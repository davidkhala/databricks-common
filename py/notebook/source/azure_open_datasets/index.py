import nyctlc
import context

nyctlc.load_raw(catalog=context.catalog, volume=context.default_volume)


# COMMAND ----------
context.copy_to_current()
