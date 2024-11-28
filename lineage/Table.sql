select entity_type,
       entity_id,
       source_type,
       source_table_full_name,
       target_type,
       target_table_full_name
from system.access.table_lineage
where source_table_full_name is not null
  and target_table_full_name is not null
;


select case
           when entity_type = 'NOTEBOOK' then nd.path
           else entity_id
           end as notebook_path,
       source_type,
       source_table_full_name,
       target_type,
       target_table_full_name
from system.access.table_lineage tl
left join
    global_temp.notebooks_dimension nd
on
    cl.entity_id = nd.object_id and cl.entity_type = 'NOTEBOOK'
where source_table_full_name is not null
  and target_table_full_name is not null
;