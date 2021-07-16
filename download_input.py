import pandas as pd
import sqlalchemy
from sqlalchemy_hana import dialect

#This line prevents the bundled .exe from throwing a sqlalchemy-related error
sqlalchemy.dialects.registry.register('hana', 'sqlalchemy_hana.dialect', 'HANAHDBCLIDialect')

connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false')

with connection_to_HANA.connect() as con:
    pd.read_sql_table('breakout_file', con, schema = 'anylogic').to_excel('database_input/input/breakout_file.xlsx', index = False)
    pd.read_sql_table('bulk_inventory', con, schema = 'anylogic').to_excel('database_input/input/bulk_inventory.xlsx', index = False)
    pd.read_sql_table('demand', con, schema = 'anylogic').to_excel('database_input/input/demand.xlsx', index = False)
    pd.read_sql_table('extruders', con, schema = 'anylogic').to_excel('database_input/input/extruders.xlsx', index = False)
    pd.read_sql_table('packlines', con, schema = 'anylogic').to_excel('database_input/input/packlines.xlsx', index = False)
    pd.read_sql_table('bins', con, schema = 'manual_files').to_excel('database_input/manual_files/bins.xlsx', index = False)
    pd.read_sql_table('change_over', con, schema = 'manual_files').to_excel('database_input/manual_files/change_over.xlsx', index = False)
    pd.read_sql_table('customer_priority', con, schema = 'manual_files').to_excel('database_input/manual_files/customer_priority.xlsx', index = False)
    pd.read_sql_table('dry_liq_digest_reward', con, schema = 'manual_files').to_excel('database_input/manual_files/dry_liq_digest_reward.xlsx', index = False)
    pd.read_sql_table('extruders_schedule', con, schema = 'manual_files').to_excel('database_input/manual_files/extruders_schedule.xlsx', index = False)
    pd.read_sql_table('families', con, schema = 'manual_files').to_excel('database_input/manual_files/families.xlsx', index = False)
    pd.read_sql_table('finished_good', con, schema = 'manual_files').to_excel('database_input/manual_files/finished_good.xlsx', index = False)
    pd.read_sql_table('md_bulk_code', con, schema = 'manual_files').to_excel('database_input/manual_files/md_bulk_code.xlsx', index = False)
    pd.read_sql_table('model_workcenters', con, schema = 'manual_files').to_excel('database_input/manual_files/model_workcenters.xlsx', index = False)
    pd.read_sql_table('product_priority', con, schema = 'manual_files').to_excel('database_input/manual_files/product_priority.xlsx', index = False)
    pd.read_sql_table('resources', con, schema = 'manual_files').to_excel('database_input/manual_files/resources.xlsx', index = False)
    pd.read_sql_table('units_per_pallet', con, schema = 'manual_files').to_excel('database_input/manual_files/units_per_pallet.xlsx', index = False)
    pd.read_sql_table('opt_seed_values', con, schema = 'output').to_excel('database_input/output/opt_seed_values.xlsx', index = False)
    pd.read_sql_table('out_due_date_backlog', con, schema = 'output').to_excel('database_input/output/out_due_date_backlog.xlsx', index = False)
    pd.read_sql_table('out_warehouse', con, schema = 'output').to_excel('database_input/output/out_warehouse.xlsx', index = False)
    pd.read_sql_table('schedule_bulk', con, schema = 'output').to_excel('database_input/output/schedule_bulk.xlsx', index = False)
    pd.read_sql_table('schedule_sku', con, schema = 'output').to_excel('database_input/output/schedule_sku.xlsx', index = False)
    pd.read_sql_table('sku_inventory', con, schema = 'output').to_excel('database_input/output/sku_inventory.xlsx', index = False)