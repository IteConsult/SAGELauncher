import pandas as pd

table_urls = {'BOM': r'http://10.4.240.65/api/IntegrationAPI/GetBOM',
              'Inventory': r'http://10.4.240.65/api/IntegrationAPI/GetInventory',
              'Facility': r'http://10.4.240.65/api/IntegrationAPI/GetItemFacility',
              'ItemMaster': r'http://10.4.240.65/api/IntegrationAPI/GetItemMstr',
              'RoutingAndRates': r'http://10.4.240.65/api/IntegrationAPI/GetRoutingAndRates',
              'WorkOrders': r'http://10.4.240.65/api/IntegrationAPI/GetWorkOrders',
              'WorkCenters': r'http://10.4.240.65/api/IntegrationAPI/GetWorkCenters',}

#Save raw SAGE tables to Excel files
for table in table_urls:
    try:
        globals()[table] = pd.read_json(table_urls[table], dtype = str)
        globals()[table].to_excel(f'REST_files/{table}.xlsx', index = False)
        print(f'Table {table} succesfully loaded.')
    except Exception as E:
        print('Couldn\'t load table: %s' % table)
        print(E)
        
import sqlalchemy
connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()

#Upload raw SAGE tables into HANA
for table in table_urls:
    try:
        connection_to_HANA.execute(f'DELETE FROM "SAGE".{table}')
        globals()[table].to_sql(table.lower(), con = connection_to_HANA, if_exists = 'append', index = False, schema = 'sage')
        print(f'Table {table} was uploaded to HANA succesfully.')
    except Exception as e:
        print(f'Couldn\'t save {table} table into HANA. ' + str(e))