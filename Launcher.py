#Buttons:
#*Update database from SAGE and generate model files
#*Generate model files
#*Run simulation
#*Run optimization
#*Save outputs

#TODO modificar todos los códigos teniendo en cuenta que los items no son únicos

import subprocess
import tkinter as tk
from tkinter import ttk
import pandas as pd
import threading
import sqlalchemy
import datetime
#import sqlalchemy_hana

connection_to_HANA = None

def connectToHANA():
    global connection_to_HANA
    if not connection_to_HANA:
        try:
            connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
            print('Connection established.')
        except Exception as e:
            print('Connection failed! ' + str(e))      

def generate_breakout_file(BOM, ItemMaster, Facility):
    #Merging with ItemMaster
    BREAKOUT = BOM[['ItemNumber', 'Facility', 'BomCode', 'ComponentItemNumber', 'Quantity']].merge(ItemMaster[['ItemNumber', 'CategoryCode', 'ProductType', 'DefaultFacility']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left', validate = 'm:1')
    #Keep only BOM from DefaultFacility
    BREAKOUT = BREAKOUT.query('Facility == DefaultFacility')
    #Filter Bomcodes according to Facility
    bomcode_table = {'20001': '40', '20006': '45', '20005': '40'}
    bom_filter = BREAKOUT['BomCode'] == BREAKOUT['DefaultFacility'].map(bomcode_table)
    BREAKOUT = BREAKOUT[bom_filter]
    #Keep FG items only
    BREAKOUT = BREAKOUT.query('CategoryCode == "FG"').drop('CategoryCode', axis = 1)
    #Second merging with ItemMaster to keep only 'INT' category component items
    BREAKOUT = BREAKOUT.merge(ItemMaster[['ItemNumber', 'CategoryCode']].groupby('ItemNumber').first(), left_on = 'ComponentItemNumber', right_on = 'ItemNumber', suffixes = ['','_y'])
    BREAKOUT = BREAKOUT.query('CategoryCode == "INT"').drop('CategoryCode', axis = 1)
    #Blend percentage calculation
    BREAKOUT['Quantity'] = BREAKOUT['Quantity'].astype(float)
    BREAKOUT = BREAKOUT[['ItemNumber', 'Facility', 'BomCode', 'Quantity']].groupby(['ItemNumber', 'Facility', 'BomCode']).sum().rename({'Quantity': 'BlendPercentage'}, axis = 1).query('BlendPercentage != 0').merge(BREAKOUT, left_index = True, right_on = ['ItemNumber', 'Facility', 'BomCode'], how = 'right')
    BREAKOUT['BlendPercentage'] = BREAKOUT['Quantity']/BREAKOUT['BlendPercentage']
    #Weight is sum of quantities
    BREAKOUT.rename({'Quantity': 'Weight'}, axis = 1, inplace = True)
    #Generate missing columns
    BREAKOUT['Family'] = 'NONE'
    BREAKOUT['color'] = 0
    BREAKOUT['shape'] = 0
    BREAKOUT['Type-Shape-Size Concat'] = BREAKOUT['ProductType'].copy()
    BREAKOUT['Dry-Liquid-Digest Concat'] = 0
    BREAKOUT['Family Sequence'] = 0
    BREAKOUT['Max Run Size (lb)'] = 0
    #Set column order
    BREAKOUT = BREAKOUT[['ItemNumber', 'Family', 'ComponentItemNumber', 'color', 'shape', 'ProductType', 'Type-Shape-Size Concat', 'BlendPercentage', 'Weight']]
    #Change column names
    BREAKOUT.rename({'ItemNumber': 'Finished good', 'ComponentItemNumber': 'Component formula', 'ProductType': 'Category', 'BlendPercentage': 'Blend percentage'}, axis = 1, inplace = True)    

    return BREAKOUT

def generate_packlines_and_extruders(RoutingAndRates, ItemMaster, Model_WorkCenters, Facility):
    #Filter Bomcodes according to Facility
    bomcode_table = {'20001': '40', '20006': '45', '20005': '40'}
    bomcode_filter = RoutingAndRates['BomCode'] == RoutingAndRates['Facility'].map(bomcode_table)
    #Keep only those workcenters with UseStatus equal to 2 and are either packlines or extruders
    RATES = RoutingAndRates[bomcode_filter].query('UseStatus == "2"').merge(Model_WorkCenters[['WorkCenter', 'Model Workcenter', 'Model plant', 'Area', 'Isolate']], on = 'WorkCenter', how = 'left')
    RATES = RATES.query('Area == "PACK" or Area == "EXTR"')
    #Merging with ItemMaster
    RATES = RATES.merge(ItemMaster[['ItemNumber', 'ItemWeight', 'CategoryCode']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'inner')
    #Filter by category
    RATES = RATES.query('(Area == "PACK" and CategoryCode == "FG") or (Area == "EXTR" and CategoryCode == "INT")').copy()
    RATES['OperationTimeUnits'] = RATES['OperationTimeUnits'].map({'1': '1', '2': '60'})
    RATES = RATES.astype({'BaseQuantity': float, 'ItemWeight': float, 'OperationTimeUnits': int, 'OperationTime': float})
    RATES['Pounds/hour'] = RATES['BaseQuantity']*RATES['ItemWeight']/(RATES['OperationTimeUnits']*RATES['OperationTime'])
    #Drop those for which the calculation failed (usually because weight is missing)
    RATES.dropna(subset=['Pounds/hour'], inplace = True)
    #Drop columns that won't be needed anymore
    RATES.drop(['UseStatus', 'OperationNumber', 'OperationUOM', 'OperationTime', 'OperationTimeUnits', 'BaseQuantity', 'ItemWeight'], axis = 1, inplace = True)
    #Split the dataframe
    PACKLINES = RATES.query('Area == "PACK"').drop(['Area', 'CategoryCode'], axis = 1)
    EXTRUDERS = RATES.query('Area == "EXTR"').drop(['Area', 'CategoryCode'], axis = 1)
    #Add Shrinkage column to EXTRUDERS table
    EXTRUDERS['Shrinkage'] = 0.001
    #Bring MinRunSize column from Facility table
    EXTRUDERS = EXTRUDERS.merge(Facility.query('ItemStatus == "1"')[['ItemNumber', 'ItemFacility', 'FormulaMinRunSize']], left_on = ['ItemNumber', 'Facility'], right_on = ['ItemNumber', 'ItemFacility'], how = 'left')
    #Rename columns
    EXTRUDERS = EXTRUDERS.rename({'ItemNumber': 'Component Formula', 'Model plant': 'Location', 'Model Workcenter': 'Extruder', 'WorkCenter': 'Code'}, axis = 1)
    PACKLINES = PACKLINES.rename({'ItemNumber': 'Finished Good', 'Model Workcenter': 'Packline', 'WorkCenter': 'Code', 'Model plant': 'plant'}, axis = 1)
    PACKLINES = PACKLINES[['Finished Good', 'plant', 'Packline', 'Code', 'Pounds/hour', 'Isolate']]
    EXTRUDERS = EXTRUDERS[['Component Formula', 'Location', 'Extruder', 'Code', 'Pounds/hour', 'Shrinkage', 'FormulaMinRunSize']]
    return PACKLINES, EXTRUDERS

def generate_demand(WorkOrders, ItemMaster, Model_WorkCenters):
    #TODO validar que todos los FG estén en el Breakout
    DEMAND = WorkOrders.merge(ItemMaster[['ItemNumber', 'Description', 'CategoryCode']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    fg_filter = DEMAND['CategoryCode'] == 'FG'
    DEMAND = DEMAND[fg_filter]
    DEMAND['Customer'] = 0
    #TODO traer la Formula cuando la manden los de Alphia
    DEMAND['Formula'] = 0
    DEMAND['Due date'] = pd.to_datetime(DEMAND['PlannedEnd'])
    DEMAND['Raw material date'] = DEMAND['Due date'] - datetime.timedelta(15)
    DEMAND['Inventory'] = 0
    DEMAND['Priority product'] = 0
    DEMAND['Priority'] = 0
    DEMAND = DEMAND.merge(Model_WorkCenters[['WorkCenter', 'Model plant']], on = 'WorkCenter', how = 'left')
    DEMAND['Purchase order'] = 'missing'
    DEMAND['Entity'] = 'CJFoods'
    DEMAND['Sales order'] = 'missing'
    #TODO traer el original due date cuando lo manden los de Alphia
    DEMAND['Original due date'] = pd.to_datetime(DEMAND['PlannedEnd'])
    #TODO corregir process date?
    DEMAND['Process date'] = 'week ' + str(datetime.date.today().isocalendar()[1] + 3)
    DEMAND = DEMAND.astype({'PlannedQty': float, 'CompletedQty': float})
    DEMAND['Demand quantity (pounds)'] = DEMAND['PlannedQty'] - DEMAND['CompletedQty']
    DEMAND.rename({'ItemNumber': 'Finished good', 'CustomerName': 'Customer', 'Model plant': 'Location', 
                   'WorkCenter': 'Packline', 'WorkOrderNumber': 'Work order'}, axis = 1, inplace = True)
    DEMAND = DEMAND[['Finished good', 'Description', 'Customer', 'Formula', 'Inventory', 'Priority product', 'Priority', 'Raw material date', 
                     'Demand quantity (pounds)', 'Due date', 'Location', 'Purchase order', 'Sales order', 'Original due date',
                     'Entity', 'Work order', 'Packline', 'Process date', 'Facility']]

    return DEMAND

def generate_inventory_bulk(Inventory, ItemMaster, Facility, Demand, Breakout):
    #TODO hay que repensarla cuando arreglen lo de la Facility en el ItemMaster
    INVENTORY_BULK = Inventory.query('ItemStatus == "A"').merge(ItemMaster[['ItemNumber', 'CategoryCode']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    INVENTORY_BULK.query('CategoryCode == "INT"', inplace = True)
    INVENTORY_BULK.drop('ItemStatus', axis = 1, inplace = True)
    INVENTORY_BULK['StockQuantity'] = INVENTORY_BULK['StockQuantity'].astype(float)
    INVENTORY_BULK = INVENTORY_BULK.groupby(['ItemNumber', 'Facility']).sum().reset_index().rename({'StockQuantity': 'Quantity'}, axis = 1)
    #TODO Rename columns
    
    #TODO validar el inventory
        #TODO hacer que sea independiente de la Demanda y el Breakout?
        #TODO validar el ItemStatus de la tabla Facility
    VALIDATION = Demand[['Finished good', 'Facility', 'Demand quantity (pounds)']].groupby(['Finished good', 'Facility']).sum().reset_index()
    VALIDATION = VALIDATION.merge(Breakout[['Finished good', 'Component formula', 'Blend percentage']], on = 'Finished good', how = 'left')
    VALIDATION['Quantity'] = VALIDATION['Demand quantity (pounds)'].astype(float) * VALIDATION['Blend percentage'].astype(float)
    #TODO quitar después de validar la Demanda
    VALIDATION.dropna(subset = ['Component formula'], inplace = True)
    #TODO cambiar el 3 por un 2
    VALIDATION = VALIDATION.merge(Facility[['ItemNumber', 'ItemFacility', 'ItemStatus', 'Buyable']].query('ItemStatus == "1"').drop('ItemStatus', axis = 1),
                                  left_on = ['Component formula', 'Facility'], right_on = ['ItemNumber', 'ItemFacility'], how = 'left').query('Buyable == "2"')
    VALIDATION = VALIDATION[['Component formula', 'Facility', 'Quantity']].groupby(['Component formula', 'Facility']).sum().reset_index()
    
    AUX = INVENTORY_BULK.merge(VALIDATION, suffixes = ['_actual', '_needed'], left_on = ['ItemNumber', 'Facility'], right_on = ['Component formula', 'Facility'], how = 'outer')

    return INVENTORY_BULK
    
def generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters, Inventory, WorkCenters):

    #Model files generation and uploading

    #1) Breakout
    try:
        BREAKOUT = generate_breakout_file(BOM, ItemMaster, Facility)
        print('Breakout table succesfully generated.')
    except Exception as e:
        print('Failed to generate Breakout table: ' + str(e))
    else:
        try:
            BREAKOUT.to_sql('breakout_file', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Breakout table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Breakout table to HANA: ' + str(e))
    #2) Packlines and extruders
    try:
        PACKLINES, EXTRUDERS = generate_packlines_and_extruders(RoutingAndRates, ItemMaster, Model_WorkCenters, Facility)
        print('Packlines and Extruders tables succesfully generated.')
    except Exception as e:
        print('Failed to generate Packlines and Extruders tables: ' + str(e))
    else:
        try:
            PACKLINES.to_sql('packlines', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Packlines table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Packlines table to HANA: ' + str(e))
        try:
            EXTRUDERS.to_sql('extruders', con = connection_to_HANA, if_exists ='replace', index = False)
            print('Extruders table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Extruders table to HANA: ' + str(e))
    #3) Demand
    try:
        DEMAND = generate_demand(WorkOrders, ItemMaster, Model_WorkCenters)
        print('Demand table succesfully generated.')
    except Exception as e:
        print('Failed to generate Demand table: ' + str(e))
    else:
        try:
            DEMAND.to_sql('demand', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Demand table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Demand table to HANA: ' + str(e))
    #4) Inventory bulk (es importante que el Inventory bulk se cree después que la demanda y el breakout para poder validarlo)
    try:
        INVENTORY_BULK = generate_inventory_bulk(Inventory, ItemMaster, Facility, DEMAND, BREAKOUT)
        print('Inventory bulk table succesfully generated.')
    except Exception as e:
        print('Failed to generate Inventory bulk table: ' + str(e))
    else:
        try:
            INVENTORY_BULK.to_sql('inventory_bulk', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace')
            print('Inventory bulk table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Inventory bulk table to HANA: ' + str(e))

def update_db_from_SAGE():
    
    #Connect to HANA
    connectToHANA()

    table_urls = {'BOM': r'http://10.4.240.65/api/IntegrationAPI/GetBOM',
              'Inventory': r'http://10.4.240.65/api/IntegrationAPI/GetInventory',
              'Facility': r'http://10.4.240.65/api/IntegrationAPI/GetItemFacility',
              'ItemMaster': r'http://10.4.240.65/api/IntegrationAPI/GetItemMstr',
              'RoutingAndRates': r'http://10.4.240.65/api/IntegrationAPI/GetRoutingAndRates',
              'WorkCenters': r'http://10.4.240.65/api/IntegrationAPI/GetWorkCenters',
              'WorkOrders': r'http://10.4.240.65/api/IntegrationAPI/GetWorkOrders'}

    #Reads tables from REST services
    for table in table_urls:
        try:
            globals()[table] = pd.read_json(table_urls[table], dtype = str)
            print(f'Table {table} succesfully loaded.')
        except Exception as e:
            print(f'Couldn\'t load table {table}: ' + str(e))
            #try to read backup from HANA?

    #Upload raw SAGE tables into HANA
    for table in table_urls:
        try:
            connection_to_HANA.execute(f'DELETE FROM {table}')
            globals()[table].to_sql(table.lower(), con = connection_to_HANA, if_exists = 'append', index = False, schema = 'sage')
            print(f'Table {table} was uploaded to HANA succesfully.')
        except Exception as e:
            print(f'Couldn\'t save {table} table into HANA. ' + str(e))

    #Read manual files from HANA
    manual_files = ['Model_WorkCenters']

    for table in manual_files:
        globals()[table] = pd.read_sql_table(table.lower(), schema = 'manual_files', con = connection_to_HANA)

    generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters, Inventory, WorkCenters)

def generate_model_files_from_backup():

    #Connect to HANA
    connectToHANA()

    tables = ['BOM', 'Inventory', 'Facility', 'ItemMaster', 'RoutingAndRates', 'WorkCenters', 'WorkOrders']

    #Read SAGE tables from HANA
    for table in tables:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'sage', con = connection_to_HANA)
            print(f'Table {table} succesfully read from HANA.')
        except Exception as e:
            print('Couldn\'t read table {table} from HANA. ' + str(e))

    #Read manual files from HANA
    manual_files = ['Model_Workcenters']

    for table in manual_files:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'manual_files', con = connection_to_HANA)
            print(f'Table {table} succesfully read from HANA.')
        except Exception as e:
            print('Couldn\'t read table {table} from HANA. ' + str(e))

    generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_Workcenters, Inventory, WorkCenters)

def update_db_from_SAGE_command():
    update_db_from_SAGE_thread = threading.Thread(target = update_db_from_SAGE, daemon = True)
    update_db_from_SAGE_thread.start()
    update_db_from_SAGE_pgb.start()
    update_db_from_SAGE_thread.join()
    update_db_from_SAGE_pgb.stop()

def generate_model_files_from_backup_command():
    generate_model_files_thread = threading.Thread(target = generate_model_files_from_backup, daemon = True)
    generate_model_files_thread.start()
    generate_model_files_pgb.start()
    generate_model_files_thread.join()
    generate_model_files_pgb.stop()
        
def run_experiment(experiment):
    subprocess.run(f'Model\CJFoods_windows-{experiment}.bat')
    
window = tk.Tk()
window.title('Alphia Launcher')
#window.state("zoomed")

update_db_from_SAGE_btn = tk.Button(text = 'Update database and model files from SAGE', command = lambda: threading.Thread(target = update_db_from_SAGE_command, daemon = True).start())
update_db_from_SAGE_btn.pack(pady = 10)

update_db_from_SAGE_pgb = ttk.Progressbar(mode = 'indeterminate')
update_db_from_SAGE_pgb.pack(pady = 10)

generate_model_files_btn = tk.Button(text = 'Generate new model files from HANA backup', command = lambda: threading.Thread(target = generate_model_files_from_backup_command, daemon = True).start())
generate_model_files_btn.pack(pady = 10)

generate_model_files_pgb = ttk.Progressbar(mode = 'indeterminate')
generate_model_files_pgb.pack(pady = 10)

run_simulation_btn = tk.Button(text = 'Run simulation', command = lambda: threading.Thread(target = run_experiment, args = ('simulation',), daemon = True).start())
run_simulation_btn.pack(pady = 10)

run_optimization_btn = tk.Button(text = 'Run optimization', command = lambda: threading.Thread(target = run_experiment, args = ('optimization',), daemon = True).start())
run_optimization_btn.pack(pady = 10)

window.mainloop()

connection_to_HANA.close()