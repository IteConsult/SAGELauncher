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
import random
import numpy as np
#import sqlalchemy_hana

connection_to_HANA = None
NOT_IN_BREAKOUT = None
NOT_IN_PACKLINES = None
NOT_IN_EXTRUDERS = None

def connectToHANA():
    global connection_to_HANA
    if not connection_to_HANA:
        try:
            connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
            print('Connection established.')
        except Exception as e:
            print('Connection failed! ' + str(e))      

def generate_breakout_file(BOM, ItemMaster, Facility):
    #TODO PROVISORIO Lleno la columna DefaultFacility con cosas al azar
    ItemMaster['DefaultFacility'] = 0
    ItemMaster['DefaultFacility'] = ItemMaster['DefaultFacility'].apply(lambda x: random.choice(['20001', '20005', '20006']))
    #Merging with ItemMaster
    BREAKOUT = BOM[['ItemNumber', 'Facility', 'BomCode', 'ComponentItemNumber', 'Quantity']].merge(ItemMaster[['ItemNumber', 'CategoryCode', 'ProductType', 'DefaultFacility', 'ItemWeight', 'BagWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left', validate = 'm:1')
    BREAKOUT.dropna(subset = ['Facility', 'BomCode', 'ComponentItemNumber', 'Quantity', 'CategoryCode', 'DefaultFacility', 'ItemWeight', 'BagWeight'], inplace = True)
    #Keep only BOM from DefaultFacility
    BREAKOUT = BREAKOUT.query('Facility == DefaultFacility')
    #Filter Bomcodes according to Facility
    bomcode_table = {'20001': '20', '20006': '45', '20005': '40'}
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
    #Weight is BagSize
    BREAKOUT.rename({'BagWeight': 'Weight'}, axis = 1, inplace = True)
    BREAKOUT['Weight'] = BREAKOUT['Weight'].astype(float)
    #Generate missing columns
    BREAKOUT['Family'] = 'NONE'
    BREAKOUT['color'] = 0
    BREAKOUT['shape'] = 0
    BREAKOUT['Type-Shape-Size Concat'] = BREAKOUT['ProductType'].copy()
    BREAKOUT['Dry-Liquid-Digest Concat'] = '0 - 0'
    BREAKOUT['Family Sequence'] = 0
    BREAKOUT['Max Run Size (lb)'] = 0
    #TODO Provisorio (después va a estar en el Extruders cuando se acople el modelo)
    BREAKOUT = BREAKOUT.merge(Facility[['ItemNumber', 'FormulaMinRunSize']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    BREAKOUT['FormulaMinRunSize'] = BREAKOUT['FormulaMinRunSize'].astype(float).fillna(0)
    #Set column order
    BREAKOUT = BREAKOUT[['ItemNumber', 'Family', 'ComponentItemNumber', 'color', 'shape', 'ProductType', 'Type-Shape-Size Concat', 'Dry-Liquid-Digest Concat', 
                'BlendPercentage', 'Family Sequence', 'Max Run Size (lb)', 'Weight', 'FormulaMinRunSize']]
    #Change column names
    BREAKOUT.rename({'ItemNumber': 'Finished good', 'ComponentItemNumber': 'Component formula', 'ProductType': 'Category', 'BlendPercentage': 'Blend percentage'}, axis = 1, inplace = True)    
    #TODO PROVISORIO Esto es para traer el type desde JDE. Funciona solamente si tenemos presente la tabla "MD_Bulk_Code.csv" de SQL_Files. Pedirla a Juan si no la tenés.
    JDE = pd.read_csv('MD_Bulk_Code.csv')
    BREAKOUT = BREAKOUT.merge(ItemMaster[['ItemNumber', 'LegacyCJFCode']].groupby('ItemNumber').first(), left_on = 'Component formula', right_on = 'ItemNumber', how = 'left')
    BREAKOUT = BREAKOUT.merge(JDE[['ID', 'Type']], left_on = 'LegacyCJFCode', right_on = 'ID', how = 'left')
    BREAKOUT['Category'] = BREAKOUT['Type'].copy()
    BREAKOUT['Type-Shape-Size Concat'] = BREAKOUT['Type'].copy()
    
    
    #TODO provisorio ----
    JDE_FG = pd.read_csv("Finished_Good.csv", header=0, index_col=False, keep_default_na=True)
    JDE_FG = JDE_FG[(JDE_FG.Account == "lb/Unit") & (JDE_FG.Measure == 500)]
    JDE_FG = JDE_FG[["Item_Code", "Measure"]]
    BREAKOUT = BREAKOUT.drop("LegacyCJFCode", axis=1)
    BREAKOUT = BREAKOUT.merge(ItemMaster[["ItemNumber", "LegacyCJFCode"]].groupby("ItemNumber").first(), left_on="Finished good", right_on="ItemNumber", how="left") 
    BREAKOUT = BREAKOUT.merge(JDE_FG, left_on="LegacyCJFCode", right_on="Item_Code", how="left")
    BREAKOUT["Weight"] = np.where(BREAKOUT["Measure"] == 500, "500", BREAKOUT["Weight"])
    BREAKOUT.drop(['ID','Type', 'Item_Code', 'Measure', "LegacyCJFCode"], axis = 1, inplace = True)
    # --------------------
    return BREAKOUT

def generate_packlines_and_extruders(RoutingAndRates, ItemMaster, Model_WorkCenters, Facility):
    #Filter Bomcodes according to Facility
    bomcode_table = {'20001': '20', '20006': '45', '20005': '40'}
    bomcode_filter = RoutingAndRates['BomCode'] == RoutingAndRates['Facility'].map(bomcode_table)
    #Keep only those workcenters with UseStatus equal to 2 and are either packlines or extruders
    RATES = RoutingAndRates[bomcode_filter].query('UseStatus == "2"').merge(Model_WorkCenters[['WorkCenter', 'Model Workcenter', 'Model plant', 'Area', 'Isolate']], on = 'WorkCenter', how = 'inner')
    RATES['Isolate'] = RATES['Isolate'].astype(int).astype(bool)
    RATES = RATES.query('Area == "PACK" or Area == "EXTR"')
    #Merging with ItemMaster
    RATES = RATES.merge(ItemMaster[['ItemNumber', 'ItemWeight', 'CategoryCode', 'LegacyCJFCode']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'inner')
    
    #TODO provisorio -----
    JDE_FG = pd.read_csv("Finished_Good.csv", header=0, index_col=False, keep_default_na=True)
    JDE_FG = JDE_FG[(JDE_FG.Account == "lb/Unit") & (JDE_FG.Measure == 500)]
    JDE_FG = JDE_FG[["Item_Code", "Measure"]]
    RATES = RATES.merge(JDE_FG, left_on="LegacyCJFCode", right_on="Item_Code", how="left")
    RATES["ItemWeight"] = np.where((RATES["Measure"] == 500) & (RATES["OperationTime"].astype(float) > 0.01), "500", RATES["ItemWeight"])
    RATES.drop(['Measure', "LegacyCJFCode"], axis = 1, inplace = True)
    #-----
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
    #Add Facility code column to Packlines table
    PACKLINES['Facility code'] = PACKLINES['Model plant'].map({'PC10': '20005', 'PC30': '20005', 'Bern': '20001', 'BAXTER SP': '20006'})
    #Add Shrinkage column to EXTRUDERS table
    EXTRUDERS['Shrinkage'] = 0.001
    #Bring MinRunSize column from Facility table
    #TODO PROVISORIO el siguiente renglón comentado
    #EXTRUDERS = EXTRUDERS.merge(Facility.query('ItemStatus == "1"')[['ItemNumber', 'ItemFacility', 'FormulaMinRunSize']], left_on = ['ItemNumber', 'Facility'], right_on = ['ItemNumber', 'ItemFacility'], how = 'left')
    #TODO PROVISORIO los rates que son menores a 4000 los seteo en 4000
    PACKLINES['Pounds/hour'] = PACKLINES['Pounds/hour'].astype(float).apply(lambda x: 4000 if x < 4000 else x)
    EXTRUDERS['Pounds/hour'] = EXTRUDERS['Pounds/hour'].astype(float).apply(lambda x: 4000 if x < 4000 else x)
    #Rename columns
    EXTRUDERS = EXTRUDERS.rename({'ItemNumber': 'Component formula', 'Model plant': 'Location', 'Model Workcenter': 'Extruder', 'WorkCenter': 'Code'}, axis = 1)
    PACKLINES = PACKLINES.rename({'ItemNumber': 'Finished good', 'Model Workcenter': 'Packline', 'WorkCenter': 'Code', 'Model plant': 'plant'}, axis = 1)
    PACKLINES = PACKLINES[['Finished good', 'plant', 'Packline', 'Code', 'Pounds/hour', 'Isolate', 'Facility code']]
    #TODO PROVISORIO Descomentar esto...
    #EXTRUDERS = EXTRUDERS[['Component formula', 'Location', 'Extruder', 'Code', 'Pounds/hour', 'Shrinkage', 'FormulaMinRunSize']]
    #TODO PROVISORIO y comentar esto
    EXTRUDERS = EXTRUDERS[['Component formula', 'Location', 'Extruder', 'Code', 'Pounds/hour', 'Shrinkage']]

    return PACKLINES, EXTRUDERS

def generate_demand(WorkOrders, ItemMaster, Model_WorkCenters, BREAKOUT, PACKLINES, EXTRUDERS):    
    #TODO PROVISORIO
    global NOT_IN_BREAKOUT
    global NOT_IN_PACKLINES
    global NOT_IN_EXTRUDERS
    DEMAND = WorkOrders.merge(ItemMaster[['ItemNumber', 'Description', 'CategoryCode', 'ItemWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    #Filter only FG items
    DEMAND = DEMAND.query('CategoryCode == "FG"').copy()
    #Filter items not in Brekaout
    DEMAND['in_breakout'] = DEMAND['ItemNumber'].isin(BREAKOUT['Finished good'].values)
    NOT_IN_BREAKOUT = DEMAND.query('in_breakout == False').copy()[['ItemNumber']]
    DEMAND = DEMAND.query('in_breakout == True').copy()
    #Keep only FG whose every CF has extrusion rate (TODO or is a buyable)
    CFS_IN_DEMAND = DEMAND[['ItemNumber']].merge(BREAKOUT[['Finished good', 'Component formula']], left_on = 'ItemNumber', right_on = 'Finished good', how = 'left')
    CFS_IN_DEMAND = CFS_IN_DEMAND.merge(EXTRUDERS[['Component formula']].groupby('Component formula', as_index = False).first(), on = 'Component formula', how = 'left', indicator = True)
    NOT_IN_EXTRUDERS = CFS_IN_DEMAND.query('_merge == "left_only"').copy()[['Component formula', 'Finished good']]
    CFS_IN_DEMAND = CFS_IN_DEMAND[['Finished good', '_merge']].replace({'both': True, 'left_only': False}).groupby('Finished good').all()
    DEMAND = DEMAND.merge(CFS_IN_DEMAND.query('_merge == True').copy(), left_on = 'ItemNumber', right_on = 'Finished good', how = 'inner').drop('_merge', axis = 1)
    #Bring Model plant column
    DEMAND = DEMAND.merge(Model_WorkCenters[['WorkCenter', 'Model plant']], on = 'WorkCenter', how = 'left')
    #Keep only FG with packing rate in plant
    DEMAND = DEMAND.merge(PACKLINES[['Finished good', 'plant']], left_on = ['ItemNumber', 'Model plant'], right_on = ['Finished good', 'plant'], how = 'left', indicator = True)
    NOT_IN_PACKLINES = DEMAND.query('_merge == "left_only"').copy()
    #TODO traer el Customer cuando lo manden los de Alphia
    DEMAND['Customer'] = 0
    #TODO traer la Formula cuando la manden los de Alphia
    DEMAND['Formula'] = 0
    DEMAND['Due date'] = pd.to_datetime(DEMAND['PlannedEnd'])
    DEMAND['Raw material date'] = DEMAND['Due date'] - datetime.timedelta(15)
    DEMAND['Inventory'] = 0
    DEMAND['Priority product'] = 0
    DEMAND['Priority'] = 0
    DEMAND['Purchase order'] = 'missing'
    DEMAND['Entity'] = 'CJFoods'
    DEMAND['Sales order'] = 'missing'
    #TODO traer el original due date cuando lo manden los de Alphia
    DEMAND['Original due date'] = pd.to_datetime(DEMAND['PlannedEnd'])
    #TODO corregir process date?
    DEMAND['Process date'] = 'week ' + str(datetime.date.today().isocalendar()[1] + 3)
    DEMAND = DEMAND.astype({'PlannedQty': float, 'CompletedQty': float, 'ItemWeight': float})
    DEMAND['Demand quantity (pounds)'] = (DEMAND['PlannedQty'] - DEMAND['CompletedQty'])*DEMAND['ItemWeight']
    DEMAND.rename({'CustomerName': 'Customer', 'Model plant': 'Location', 'WorkCenter': 'Packline', 
                   'WorkOrderNumber': 'Work order'}, axis = 1, inplace = True)
    DEMAND = DEMAND[['Finished good', 'Description', 'Customer', 'Formula', 'Inventory', 'Priority product', 'Priority', 'Raw material date', 
                     'Demand quantity (pounds)', 'Due date', 'Location', 'Purchase order', 'Sales order', 'Original due date',
                     'Entity', 'Work order', 'Packline', 'Process date', 'Facility']]

    return DEMAND

def generate_inventory_bulk(Inventory, ItemMaster, Facility, DEMAND, BREAKOUT):
    #TODO hay que repensarla cuando arreglen lo de la Facility en el ItemMaster
    INVENTORY_BULK = Inventory.query('ItemStatus == "A"').merge(ItemMaster[['ItemNumber', 'CategoryCode', 'ItemWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    INVENTORY_BULK.query('CategoryCode == "INT"', inplace = True)
    INVENTORY_BULK.dropna(subset = ['ItemWeight'], inplace = True)
    INVENTORY_BULK['Quantity'] = INVENTORY_BULK['StockQuantity'].astype(float) * INVENTORY_BULK['ItemWeight'].astype(float)
    INVENTORY_BULK = INVENTORY_BULK.groupby(['ItemNumber', 'Facility']).sum().reset_index().rename({'ItemNumber': 'Component formula'}, axis = 1)
    #TODO Rename columns

    VALIDATION = DEMAND[['Finished good', 'Facility', 'Demand quantity (pounds)']].groupby(['Finished good', 'Facility']).sum().reset_index()
    VALIDATION = VALIDATION.merge(BREAKOUT[['Finished good', 'Component formula', 'Blend percentage']], on = 'Finished good', how = 'left')
    VALIDATION['Quantity'] = VALIDATION['Demand quantity (pounds)'].astype(float) * VALIDATION['Blend percentage'].astype(float)
    #TODO quitar después de validar la Demanda
    VALIDATION.dropna(subset = ['Component formula'], inplace = True)
    VALIDATION = VALIDATION.merge(Facility[['ItemNumber', 'ItemFacility', 'ItemStatus', 'Buyable']].query('ItemStatus == "1"').drop('ItemStatus', axis = 1),
                                  left_on = ['Component formula', 'Facility'], right_on = ['ItemNumber', 'ItemFacility'], how = 'left').query('Buyable == "2"')

    # ------------------------------
    VALIDATION = VALIDATION[['Component formula', 'Facility', 'Quantity']].groupby(['Component formula', 'Facility']).sum().reset_index()
    
    INVENTORY_BULK = INVENTORY_BULK.merge(VALIDATION, suffixes = ['_actual', '_needed'], left_on = ['Component formula', 'Facility'], right_on = ['Component formula', 'Facility'], how = 'outer').fillna(0)

    VALIDATION = INVENTORY_BULK.query('Quantity_needed > Quantity_actual')
    INVENTORY_BULK['Quantity'] = INVENTORY_BULK[['Quantity_needed', 'Quantity_actual']].max(axis = 1)
    
    INVENTORY_BULK = INVENTORY_BULK[['Component formula', 'Facility', 'Quantity']]
    
    # --- (1) en documentación! ---
    INVENTORY_BULK = INVENTORY_BULK.query('Facility != "20002"')
    INVENTORY_BULK = INVENTORY_BULK.query('Facility != "20004"')
    # -----------------------------
    
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
            BREAKOUT.to_sql('breakout_file', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace', index = False)
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
            PACKLINES.to_sql('packlines', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Packlines table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Packlines table to HANA: ' + str(e))
        try:
            EXTRUDERS.to_sql('extruders', schema = 'anylogic', con = connection_to_HANA, if_exists ='replace', index = False)
            print('Extruders table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Extruders table to HANA: ' + str(e))
    #3) Demand (must be created after Breakout, Packlines and Extruders in order to validate)
    try:
        DEMAND = generate_demand(WorkOrders, ItemMaster, Model_WorkCenters, BREAKOUT, PACKLINES, EXTRUDERS)
        print('Demand table succesfully generated.')
    except Exception as e:
        print('Failed to generate Demand table: ' + str(e))
    else:
        try:
            DEMAND.to_sql('demand', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Demand table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Demand table to HANA: ' + str(e))
    #4) Inventory bulk (must be created after Demand in order to validate)
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

def generate_model_files_from_backup(): #TODO cambiar la lógica: en vez de leer las tablas de HANA, que lea las tablas locales (editadas con pandastable)

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
    
def generate_excels():
    rest_tables = ['BOM', 'Inventory', 'Facility', 'ItemMaster', 'RoutingAndRates', 'WorkCenters', 'WorkOrders', 'Model_WorkCenters', 'Extruders_schedule']
    for table in rest_tables:
        globals()[table] = pd.read_excel(table+'.xlsx').astype(str)
    
    BREAKOUT = generate_breakout_file(BOM, ItemMaster, Facility)
    PACKLINES, EXTRUDERS = generate_packlines_and_extruders(RoutingAndRates, ItemMaster, Model_WorkCenters, Facility)
    DEMAND = generate_demand(WorkOrders, ItemMaster, Model_WorkCenters, BREAKOUT, PACKLINES, EXTRUDERS)
    INVENTORY_BULK = generate_inventory_bulk(Inventory, ItemMaster, Facility, DEMAND, BREAKOUT)

    BREAKOUT.to_excel('Breakout_file.xlsx', sheet_name = 'Breakout', index = False)
    PACKLINES.to_excel('Packlines.xlsx', sheet_name = 'Packlines', index = False)
    EXTRUDERS.to_excel('Extruders.xlsx', sheet_name = 'Extruders', index = False)
    with pd.ExcelWriter('Demand.xlsx') as writer:
        DEMAND.to_excel(writer, sheet_name = 'Demand', index = False)
        INVENTORY_BULK.to_excel(writer, sheet_name = 'Bulk Inventory', index = False)
        Extruders_schedule.to_excel(writer, sheet_name = 'Extruders schedule', index = False)
    with pd.ExcelWriter('Error_Demand.xlsx') as writer:
        NOT_IN_BREAKOUT.to_excel(writer, sheet_name = 'Missing from Breakout', index = False)
        NOT_IN_EXTRUDERS.to_excel(writer, sheet_name = 'Missing from Extruders', index = False)
        NOT_IN_PACKLINES.to_excel(writer, sheet_name = 'Missing from Packlines', index = False)
    
    if v.get():
        import sqlalchemy
        connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
        try:
            BREAKOUT.to_sql('breakout_file', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Breakout table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Breakout table to HANA: ' + str(e))
        try:
            PACKLINES.to_sql('packlines', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Packlines table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Packlines table to HANA: ' + str(e))
        try:
            EXTRUDERS.to_sql('extruders', schema = 'anylogic', con = connection_to_HANA, if_exists ='replace', index = False)
            print('Extruders table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Extruders table to HANA: ' + str(e))
        try:
            DEMAND.to_sql('demand', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace', index = False)
            print('Demand table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Demand table to HANA: ' + str(e))
        try:
            INVENTORY_BULK.to_sql('inventory_bulk', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace')
            print('Inventory bulk table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Inventory bulk table to HANA: ' + str(e))
        try:
            Extruders_schedule.to_sql('extruders_schedule', schema = 'anylogic', con = connection_to_HANA, if_exists = 'replace')
            print('Extruders schedule succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Extruders schedule table to HANA: ' + str(e))
        
    print('Listo!')

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

generate_excels_frame = tk.Frame(window)
generate_excels_frame.pack()

generate_excels_btn = tk.Button(generate_excels_frame, text = 'Generate Excels', command = lambda: threading.Thread(target = generate_excels, daemon = True).start())
generate_excels_btn.pack(side = tk.LEFT, pady = 10)

v = tk.IntVar()

to_HANA_checkbox = tk.Checkbutton(generate_excels_frame, text = 'To HANA', variable = v)
to_HANA_checkbox.select()
to_HANA_checkbox.pack(side = tk.RIGHT, padx = 10, pady = 10)

window.mainloop()

if connection_to_HANA:
    connection_to_HANA.close()