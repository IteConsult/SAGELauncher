#Buttons:
#*Update database from SAGE and generate model files
#*Generate model files
#*Run simulation
#*Run optimization
#*Save outputs

import time
import subprocess
import tkinter as tk
from tkinter import ttk
import pandas as pd
import threading
import sqlalchemy
import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)
from webbrowser import open as webopen
from CustomTable import CustomTable
from ManualInput import ManualInput, LoadingWindow
import traceback

#This line prevents the bundled .exe from throwing a sqlalchemy-related error
sqlalchemy.dialects.registry.register('hana', 'sqlalchemy_hana.dialect', 'HANAHDBCLIDialect')

#Debug variable
to_excel = False
#Global variables
connection_to_HANA = None
DEMAND = pd.DataFrame()

# DEMAND = ERROR_DEMAND = pd.DataFrame()

def connectToHANA():
    global connection_to_HANA
    if not connection_to_HANA:
        try:
            connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
            print('Connection to cloud database established.')
            return 'Connection succesful.'
        except Exception as e:
            print('Connection failed!\n' + traceback.format_exc())
            return 'Connection to cloud database failed!'

def update_db_from_SAGE(loading_window):
    print('update_db_from_SAGE function called.')

    #Connect to HANA
    try:
        connectToHANA()
    except:
        print('Couldn\'t connect to database!' + traceback.format_exc()) #TODO add warning message
        return 1

    table_urls = {'BOM': r'http://10.4.240.65/api/IntegrationAPI/GetBOM',
              'Inventory': r'http://10.4.240.65/api/IntegrationAPI/GetInventory',
              'Facility': r'http://10.4.240.65/api/IntegrationAPI/GetItemFacility',
              'ItemMaster': r'http://10.4.240.65/api/IntegrationAPI/GetItemMstr',
              'RoutingAndRates': r'http://10.4.240.65/api/IntegrationAPI/GetRoutingAndRates',
              'WorkCenters': r'http://10.4.240.65/api/IntegrationAPI/GetWorkCenters',
              'WorkOrders': r'http://10.4.240.65/api/IntegrationAPI/GetWorkOrders'}

    #Reads tables from REST services
    for table in table_urls:
        if connection_to_HANA:
            try:
                globals()[table] = pd.read_json(table_urls[table], dtype = str)
                print(f'Table {table} succesfully loaded.')
            except Exception as e:
                print(f'Couldn\'t load table {table}: ' + traceback.format_exc())
                #loading_window.destroy()
                return 1
                #try to read backup from HANA?
        else:
            return 1

    #Upload raw SAGE tables into HANA
    for table in table_urls:
        try:
            connection_to_HANA.execute(f'DELETE FROM "SAGE".{table}')
            globals()[table].to_sql(table.lower(), con = connection_to_HANA, if_exists = 'append', index = False, schema = 'sage')
            print(f'Table {table} was uploaded to HANA succesfully.')
        except Exception as e:
            print(f'Couldn\'t save {table} table into HANA. ' + traceback.format_exc())
            #loading_window.destroy()
            return 1

    #Read manual files from HANA
    manual_files = ['Model_WorkCenters', 'MD_Bulk_Code', 'Finished_Good', 'Product_Priority', 'Customer_Priority', 'Families']

    for table in manual_files:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'manual_files', con = connection_to_HANA).astype(str)
            print(f'Table {table} succesfully read from HANA.')
        except Exception as e:
            print('Couldn\'t read table {table} from HANA. ' + traceback.format_exc())
            #loading_window.destroy()
            return 1

    s_code = generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters,
                                                Inventory, WorkCenters, MD_Bulk_Code, Finished_Good, Families)

    print(f'generate_model_files_from_backup function finished with code {s_code}.')

    #loading_window.destroy()
    return s_code

def generate_model_files_from_backup(loading_window):
    print('generate_model_files_from_backup function called.')

    #Connect to HANA
    try:
        connectToHANA()
    except:
        print('Couldn\'t connect to database!') #TODO add warning message
        #loading_window.destroy()
        return 1

    tables = ['BOM', 'Inventory', 'Facility', 'ItemMaster', 'RoutingAndRates', 'WorkCenters', 'WorkOrders']

    #Read SAGE tables from HANA
    for table in tables:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'sage', con = connection_to_HANA).astype(str)
            print(f'Table {table} succesfully read from HANA.')
        except Exception as e:
            print(f'Couldn\'t read table {table} from HANA. ' + traceback.format_exc())
            #loading_window.destroy()
            return 1

    #Read manual files from HANA
    manual_files = ['Model_WorkCenters', 'MD_Bulk_Code', 'Finished_Good', 'Families', 'Product_Priority', 'Customer_Priority', 'Extruders_Schedule']

    for table in manual_files:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'manual_files', con = connection_to_HANA).astype(str)
            print(f'Table {table} sucesfully read from HANA.')
        except:
            print(f'Couldn\'t read {table} from HANA.' + traceback.format_exc())
            ##loading_window.destroy()
            return 1

    s_code = generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters, Inventory, WorkCenters, MD_Bulk_Code, Finished_Good, Families)

    ##loading_window.destroy()
    print(f'generate_model_files_from_backup function finished with code {s_code}.')

    return s_code

def generate_breakout_file(BOM, ItemMaster, Facility, MD_Bulk_Code, Finished_Good, Families):
    #Merging with ItemMaster
    BREAKOUT = BOM[['ItemNumber', 'Facility', 'BomCode', 'ComponentItemNumber', 'Quantity']].merge(ItemMaster[['ItemNumber', 'CategoryCode', 'ProductType', 'DefaultFacility', 'ItemWeight', 'BagWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left', validate = 'm:1')
    BREAKOUT.dropna(subset = ['ItemWeight', 'BagWeight'], inplace = True)
    #Try to keep BOM from DefaultFacility if possible
    # AVAILABLE_PLANTS = BREAKOUT[['ItemNumber', 'Facility', 'Default']].groupby('ItemNumber')
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
    #Bring Family column
    BREAKOUT = BREAKOUT.merge(Families, left_on = 'ItemNumber', right_on = 'Finished Good', how = 'left').drop('Finished Good', axis = 1)
    BREAKOUT['Family'].fillna('NONE', inplace = True)
    BREAKOUT['Family Sequence'].fillna('0', inplace = True)
    #Generate missing columns
    BREAKOUT['color'] = 0
    BREAKOUT['shape'] = 0
    BREAKOUT['Type-Shape-Size Concat'] = BREAKOUT['ProductType'].copy()
    BREAKOUT['Dry-Liquid-Digest Concat'] = '0 - 0'
    BREAKOUT['Max Run Size (lb)'] = 0
    #TODO Provisorio (después va a estar en el Extruders cuando se acople el modelo)
    BREAKOUT = BREAKOUT.merge(Facility[['ItemNumber', 'FormulaMinRunSize']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    BREAKOUT['FormulaMinRunSize'] = BREAKOUT['FormulaMinRunSize'].astype(float).fillna(0)
    #Set column order
    BREAKOUT = BREAKOUT[['ItemNumber', 'Family', 'ComponentItemNumber', 'color', 'shape', 'ProductType', 'Type-Shape-Size Concat', 'Dry-Liquid-Digest Concat',
                'BlendPercentage', 'Family Sequence', 'Max Run Size (lb)', 'Weight', 'FormulaMinRunSize']]
    #Change column names
    BREAKOUT.rename({'ItemNumber': 'Finished good', 'ComponentItemNumber': 'Component formula', 'ProductType': 'Category', 'BlendPercentage': 'Blend percentage', 'FormulaMinRunSize': 'Minimum run size'}, axis = 1, inplace = True)
    #TODO PROVISORIO Esto es para traer el type desde JDE. Funciona solamente si tenemos presente la tabla "MD_Bulk_Code.csv" de SQL_Files. Pedirla a Juan si no la tenés.
    #JDE = pd.read_csv('MD_Bulk_Code.csv')
    BREAKOUT = BREAKOUT.merge(ItemMaster[['ItemNumber', 'LegacyCJFCode']].groupby('ItemNumber').first(), left_on = 'Component formula', right_on = 'ItemNumber', how = 'left')
    BREAKOUT = BREAKOUT.merge(MD_Bulk_Code[['id', 'Type']], left_on = 'LegacyCJFCode', right_on = 'id', how = 'left')
    BREAKOUT['Category'] = BREAKOUT['Type'].copy().fillna('0').apply(lambda x: x.upper())
    BREAKOUT['Type-Shape-Size Concat'] = BREAKOUT['Type'].copy()

    #TODO provisorio ----
    #JDE_FG = pd.read_csv("Finished_Good.csv", header=0, index_col=False, keep_default_na=True)
    Finished_Good = Finished_Good[(Finished_Good.Account == "lb/Unit") & (Finished_Good.Measure == 500)]
    Finished_Good = Finished_Good[["Item_Code", "Measure"]]
    BREAKOUT = BREAKOUT.drop("LegacyCJFCode", axis=1)
    BREAKOUT = BREAKOUT.merge(ItemMaster[["ItemNumber", "LegacyCJFCode"]].groupby("ItemNumber").first(), left_on="Finished good", right_on="ItemNumber", how="left")
    BREAKOUT = BREAKOUT.merge(Finished_Good, left_on="LegacyCJFCode", right_on="Item_Code", how="left")
    BREAKOUT["Weight"] = np.where(BREAKOUT["Measure"] == 500, "500", BREAKOUT["Weight"])
    BREAKOUT.drop(['id', 'Type', 'Item_Code', 'Measure', "LegacyCJFCode"], axis = 1, inplace = True)
    # --------------------

    return BREAKOUT

def generate_packlines_and_extruders(RoutingAndRates, ItemMaster, Model_WorkCenters, Facility, Finished_Good):
    #Filter Bomcodes according to Facility
        #TODO transfer dictionary to manual table
    bomcode_table = {'20001': '20', '20006': '45', '20005': '40'}
    bomcode_filter = RoutingAndRates['BomCode'] == RoutingAndRates['Facility'].map(bomcode_table)
    RATES = RoutingAndRates.copy()[bomcode_filter]
    #Keep only those workcenters with UseStatus equal to 2
    RATES = RATES.query('UseStatus == "2"')
    #Bring data about workcenters and keep only workcenters incorporated into the model
    RATES = RATES.merge(Model_WorkCenters[['WorkCenter', 'Model Workcenter', 'Model plant', 'Area', 'Isolate']], on = 'WorkCenter', how = 'inner')
    RATES['Isolate'] = RATES['Isolate'].map({'True': 1, 'False': 0})
    #Keep only those which are either packlines or extruders
    RATES = RATES.query('Area == "PACK" or Area == "EXTR"')
    #Merging with ItemMaster
    RATES = RATES.merge(ItemMaster[['ItemNumber', 'ItemWeight', 'CategoryCode']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'inner')

    #TODO provisorio ---------------
    RATES = RATES.merge(ItemMaster[['ItemNumber', 'LegacyCJFCode']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    #JDE_FG = pd.read_csv("Finished_Good.csv", header=0, index_col=False, keep_default_na=True)
    Finished_Good = Finished_Good[(Finished_Good.Account == "lb/Unit") & (Finished_Good.Measure == 500)]
    Finished_Good = Finished_Good[["Item_Code", "Measure"]]
    RATES = RATES.merge(Finished_Good, left_on="LegacyCJFCode", right_on="Item_Code", how="left")
    RATES["ItemWeight"] = np.where((RATES["Measure"] == 500) & (RATES["OperationTime"].astype(float) > 0.01), "500", RATES["ItemWeight"])
    RATES.drop(['Measure', "LegacyCJFCode"], axis = 1, inplace = True)
    #---------------
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
    PACKLINES = PACKLINES.rename({'ItemNumber': 'Finished good', 'Model Workcenter': 'Packline', 'WorkCenter': 'Code', 'Model plant': 'Plant'}, axis = 1)
    PACKLINES = PACKLINES[['Finished good', 'Plant', 'Packline', 'Code', 'Pounds/hour', 'Isolate', 'Facility code']]
    #TODO PROVISORIO Descomentar esto...
    #EXTRUDERS = EXTRUDERS[['Component formula', 'Location', 'Extruder', 'Code', 'Pounds/hour', 'Shrinkage', 'FormulaMinRunSize']]
    #TODO PROVISORIO y comentar esto
    EXTRUDERS = EXTRUDERS[['Component formula', 'Location', 'Extruder', 'Code', 'Pounds/hour', 'Shrinkage']]

    return PACKLINES, EXTRUDERS

def generate_demand(WorkOrders, ItemMaster, Model_WorkCenters, Product_Priority, Customer_Priority, BREAKOUT, PACKLINES, EXTRUDERS):
    #TODO PROVISORIO
    DEMAND = WorkOrders.merge(ItemMaster[['ItemNumber', 'Description', 'CategoryCode', 'ItemWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    #Filter only FG items
    DEMAND = DEMAND.query('CategoryCode == "FG"').copy()
    #Calculate demand in pounds and keep those with positive demand
    DEMAND = DEMAND.astype({'PlannedQty': float, 'CompletedQty': float, 'ItemWeight': float})
    DEMAND['Demand quantity (pounds)'] = (DEMAND['PlannedQty'] - DEMAND['CompletedQty'])*DEMAND['ItemWeight']
    DEMAND = DEMAND[DEMAND['Demand quantity (pounds)'] > 0]
    #Filter items not in Brekaout
    DEMAND['in_breakout'] = DEMAND['ItemNumber'].isin(BREAKOUT['Finished good'].values)
    NOT_IN_BREAKOUT = DEMAND.query('in_breakout == False').copy()[['ItemNumber', 'WorkOrderNumber', 'Demand quantity (pounds)']].assign(Cause = 'Finished good not in breakout', ComponentFormula = 0).rename({'ItemNumber': 'ItemNumber FG', 'Demand quantity (pounds)': 'Rejected Pounds', 'ComponentFormula': 'ItemNumber INT', 'WorkOrderNumber': 'Work Order'}, axis = 1)
    NOT_IN_BREAKOUT = NOT_IN_BREAKOUT[['ItemNumber FG', 'Work Order', 'Cause', 'ItemNumber INT', 'Rejected Pounds']]
    DEMAND = DEMAND.query('in_breakout == True')
    #Keep only FG whose every CF has extrusion rate (TODO or is a buyable)
    CFS_IN_DEMAND = DEMAND[['ItemNumber', 'Demand quantity (pounds)', 'WorkOrderNumber']].merge(BREAKOUT[['Finished good', 'Component formula', 'Blend percentage']], left_on = 'ItemNumber', right_on = 'Finished good', how = 'left')
    CFS_IN_DEMAND['in_extruders'] = CFS_IN_DEMAND['Component formula'].isin(EXTRUDERS['Component formula'].unique())
    NOT_IN_EXTRUDERS = CFS_IN_DEMAND[['Finished good', 'Component formula', 'in_extruders', 'Blend percentage', 'WorkOrderNumber', 'Demand quantity (pounds)']].merge(CFS_IN_DEMAND[['Finished good', 'in_extruders']].groupby('Finished good', as_index = False).all(), on = 'Finished good', how = 'left', suffixes = ['_cf', '_fg'])
    NOT_IN_EXTRUDERS = NOT_IN_EXTRUDERS.copy().query('in_extruders_fg == False')
    NOT_IN_EXTRUDERS['Rejected Pounds'] = NOT_IN_EXTRUDERS['Demand quantity (pounds)'].astype(float) * NOT_IN_EXTRUDERS['Blend percentage'].astype(float)
    NOT_IN_EXTRUDERS['Cause'] = 'At least one component has no extrusion rate'
    NOT_IN_EXTRUDERS.rename({'Finished good': 'ItemNumber FG', 'Component formula': 'ItemNumber INT', 'WorkOrderNumber': 'Work Order'}, axis = 1, inplace = True)
    NOT_IN_EXTRUDERS = NOT_IN_EXTRUDERS[['ItemNumber FG', 'Work Order', 'Cause', 'ItemNumber INT', 'Rejected Pounds']]
    DEMAND = DEMAND.merge(CFS_IN_DEMAND[['Finished good', 'in_extruders']].groupby('Finished good').all(), left_on = 'ItemNumber', right_on = 'Finished good', how = 'left')
    DEMAND = DEMAND.copy().query('in_extruders == True').drop('in_extruders', axis = 1)
    #Bring Model plant column
    DEMAND = DEMAND.merge(Model_WorkCenters[['WorkCenter', 'Model plant']], on = 'WorkCenter', how = 'left')
    #Keep only FG with packing rate in plant
    DEMAND['in_packlines'] = DEMAND['ItemNumber'].isin(PACKLINES['Finished good'].unique())
    NOT_IN_PACKLINES = DEMAND.query('in_packlines == False').copy()
    NOT_IN_PACKLINES = NOT_IN_PACKLINES.merge(BREAKOUT[['Finished good', 'Component formula', 'Blend percentage']], left_on = 'ItemNumber', right_on = 'Finished good', how = 'left')
    NOT_IN_PACKLINES['Rejected Pounds'] = NOT_IN_PACKLINES['Demand quantity (pounds)'].astype(float) * NOT_IN_PACKLINES['Blend percentage'].astype(float)
    NOT_IN_PACKLINES.rename({'WorkOrderNumber': 'Work Order', 'Finished good': 'ItemNumber FG', 'Component formula': 'ItemNumber INT'}, axis = 1, inplace = True)
    NOT_IN_PACKLINES['Cause'] = 'Finished good has no packing rate'
    NOT_IN_PACKLINES = NOT_IN_PACKLINES[['ItemNumber FG', 'Work Order', 'Cause', 'ItemNumber INT', 'Rejected Pounds']]
    DEMAND = DEMAND.query('in_packlines == True').copy()
    #TODO traer el Customer cuando lo manden los de Alphia
    DEMAND['Customer'] = '0'
    #TODO traer la Formula cuando la manden los de Alphia
    DEMAND['Formula'] = 0
    DEMAND['Due date'] = pd.to_datetime(DEMAND['PlannedEnd'])
    DEMAND['Raw material date'] = DEMAND['Due date'] - datetime.timedelta(15)
    DEMAND['Inventory'] = 0
    #Bring Priority columns
    DEMAND = DEMAND.merge(Product_Priority, left_on = 'ItemNumber', right_on = 'Finished Good', how = 'left').drop('Finished Good', axis = 1)
    DEMAND['Product Priority'].fillna('0', inplace = True)
    DEMAND = DEMAND.merge(Customer_Priority, on = 'Customer', how = 'left')
    DEMAND['Customer Priority'].fillna('0', inplace = True)
    DEMAND['Purchase order'] = 'missing'
    DEMAND['Entity'] = 'CJFoods'
    DEMAND['Sales order'] = 'missing'
    #TODO traer el original due date cuando lo manden los de Alphia
    DEMAND['Original due date'] = pd.to_datetime(DEMAND['PlannedEnd'])
    #TODO corregir process date?
    DEMAND['Process date'] = datetime.date.today().strftime("%Y-%m-%d")
    DEMAND.rename({'ItemNumber': 'Finished good', 'CustomerName': 'Customer', 'Model plant': 'Location', 'WorkCenter': 'Packline',
                   'WorkOrderNumber': 'Work order', 'Product Priority': 'Priority product', 'Customer Priority': 'Priority'}, axis = 1, inplace = True)
    DEMAND = DEMAND[['Finished good', 'Description', 'Customer', 'Formula', 'Inventory', 'Priority product', 'Priority', 'Raw material date',
                     'Demand quantity (pounds)', 'Due date', 'Location', 'Purchase order', 'Sales order', 'Original due date',
                     'Entity', 'Work order', 'Packline', 'Process date', 'Facility']]

    ERROR_DEMAND = pd.concat([NOT_IN_BREAKOUT, NOT_IN_EXTRUDERS, NOT_IN_PACKLINES], ignore_index = True)
    ERROR_DEMAND['Process Date'] = datetime.date.today().strftime("%Y-%m-%d")
    ERROR_DEMAND['Run'] = 1

    return DEMAND, ERROR_DEMAND

def generate_inventory_bulk(Inventory, ItemMaster, Facility, Model_WorkCenters, DEMAND, BREAKOUT):
    #TODO hay que repensarla cuando arreglen lo de la Facility en el ItemMaster
    INVENTORY_BULK = Inventory.query('ItemStatus == "A"').copy()
    #Keep only Facilities that exist in the AnyLogic model
    facilities_filter = INVENTORY_BULK['Facility'].isin(Model_WorkCenters['Facility'])
    INVENTORY_BULK = INVENTORY_BULK[facilities_filter]
    INVENTORY_BULK = INVENTORY_BULK.merge(ItemMaster[['ItemNumber', 'CategoryCode', 'ItemWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
    INVENTORY_BULK.query('CategoryCode == "INT"', inplace = True)
    INVENTORY_BULK.dropna(subset = ['ItemWeight'], inplace = True)
    INVENTORY_BULK['Quantity'] = INVENTORY_BULK['StockQuantity'].astype(float) * INVENTORY_BULK['ItemWeight'].astype(float)
    INVENTORY_BULK = INVENTORY_BULK.groupby(['ItemNumber', 'Facility']).sum().reset_index().rename({'ItemNumber': 'Component formula'}, axis = 1)

    VALIDATION = DEMAND[['Finished good', 'Facility', 'Demand quantity (pounds)']].groupby(['Finished good', 'Facility']).sum().reset_index()
    VALIDATION = VALIDATION.merge(BREAKOUT[['Finished good', 'Component formula', 'Blend percentage']], on = 'Finished good', how = 'left')
    VALIDATION['Quantity'] = VALIDATION['Demand quantity (pounds)'].astype(float) * VALIDATION['Blend percentage'].astype(float)
    VALIDATION = VALIDATION.merge(Facility[['ItemNumber', 'ItemFacility', 'ItemStatus', 'Buyable']].query('ItemStatus == "1"').drop('ItemStatus', axis = 1),
                                  left_on = ['Component formula', 'Facility'], right_on = ['ItemNumber', 'ItemFacility'], how = 'left').query('Buyable == "2"')

    # ------------------------------
    VALIDATION = VALIDATION[['Component formula', 'Facility', 'Quantity']].groupby(['Component formula', 'Facility']).sum().reset_index()

    INVENTORY_BULK = INVENTORY_BULK.merge(VALIDATION, suffixes = ['_actual', '_needed'], left_on = ['Component formula', 'Facility'], right_on = ['Component formula', 'Facility'], how = 'outer').fillna(0)

    VALIDATION = INVENTORY_BULK.query('Quantity_needed > Quantity_actual')
    INVENTORY_BULK['Quantity'] = INVENTORY_BULK[['Quantity_needed', 'Quantity_actual']].max(axis = 1)

    INVENTORY_BULK = INVENTORY_BULK[['Component formula', 'Facility', 'Quantity']]

    return INVENTORY_BULK
    
def generate_wo_demand(ItemMaster, WorkOrders):
    #Create a copy
    ItemMaster_copy = ItemMaster.copy()
    WorkOrders_copy = WorkOrders.copy()
    
    #filter order status = 1 for Workorders
    filter1 = WorkOrders_copy['OrderStatus'] == '1'
    WorkOrders_copy = WorkOrders_copy[filter1]
    #rename columns
    WorkOrders_copy.rename(columns= {'Facility':'Facility Code',
                                     'Purchase_Order':'Purchase Order',
                                     'WorkCenter':'WorkCenter Code'} ,inplace=True)
    #merge dataframes
    merge = WorkOrders_copy.merge(ItemMaster_copy[['ItemNumber', 
                                                   'CategoryCode',
                                                   'ItemWeight'
                                                   ]].groupby('ItemNumber').first(), 
                                                  on='ItemNumber',
                                                  how = 'inner')
    #filter based on two conditions
    merge = merge[
                  ((merge["CategoryCode"]=="INT") & (merge["Operation"]=='20')) | 
                  ((merge["CategoryCode"]=='FG') & (merge["Operation"]=='10'))
                  ]
    #planned - complete
    merge['Demand'] = merge['PlannedQty'].astype(float) - merge['CompletedQty'].astype(float)
    #multiply weight by plannedqty for FG
    merge.loc[merge['CategoryCode'] == 'FG', 'Demand'] = merge['Demand'].astype(float) * merge['ItemWeight'].astype(float)
    #change data type
    merge['Demand'] = merge['Demand'].astype(float)
    # 0 for negative numbers
    merge.loc[merge['Demand'] < 0, 'Demand'] = 0 
    #fill null values with 0
    merge.fillna('0', inplace=True)
    #Filter negative and 0 demand
    merge = merge[merge['Demand'] > 0]
    #run and process date
    merge['Run'] = 1
    merge['Process Date'] = datetime.date.today().strftime("%Y-%m-%d")
    #keep only dates
    merge["PlannedStart"] = merge["PlannedStart"].str.split("T", n = 1, expand = True)[0]
    merge["PlannedEnd"] = merge["PlannedEnd"].str.split("T", n = 1, expand = True)[0]
    #drop columns
    merge.drop(['OrderStatus', 'Operation', 'ItemWeight', 'PlannedQty', 'CompletedQty'], axis=1, inplace = True)
    #round decimals
    merge = merge.round(1)
    #name 
    merge.name = "WO_DEMAND"
    #return dataframe
    return merge

def generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters, Inventory, 
                                    WorkCenters, MD_Bulk_Code, Finished_Good, Families):

    #Model files generation and uploading

    #1) Breakout
    try:
        BREAKOUT = generate_breakout_file(BOM, ItemMaster, Facility, MD_Bulk_Code, Finished_Good, Families)
        print('Breakout table succesfully generated.')
    except Exception as e:
        print('Failed to generate Breakout table: ' + traceback.format_exc())
        return 1
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."BREAKOUT_FILE"')
            #BREAKOUT.to_excel('Breakout_file.xlsx', index = False)
            BREAKOUT.to_sql('breakout_file', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Breakout table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Breakout table to HANA: ' + traceback.format_exc())
            return 1

    #2) Packlines and extruders
    try:
        PACKLINES, EXTRUDERS = generate_packlines_and_extruders(RoutingAndRates, ItemMaster, Model_WorkCenters, Facility, Finished_Good)
        print('Packlines and Extruders tables succesfully generated.')
    except Exception as e:
        print('Failed to generate Packlines and Extruders tables: ' + traceback.format_exc())
        return 1
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."PACKLINES"')
            #PACKLINES.to_excel('Packlines.xlsx', index = False)
            PACKLINES.to_sql('packlines', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Packlines table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Packlines table to HANA: ' + traceback.format_exc())
            return 1
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."EXTRUDERS"')
            #EXTRUDERS.to_excel('Extruders.xlsx', index = False)
            EXTRUDERS.to_sql('extruders', schema = 'anylogic', con = connection_to_HANA, if_exists ='append', index = False)
            print('Extruders table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Extruders table to HANA: ' + traceback.format_exc())
            return 1

    #3) Demand (must be created after Breakout, Packlines and Extruders in order to validate)
    try:
        global DEMAND
        DEMAND, ERROR_DEMAND = generate_demand(WorkOrders, ItemMaster, Model_WorkCenters, Product_Priority, Customer_Priority, BREAKOUT, PACKLINES, EXTRUDERS)
        print('Demand table succesfully generated.')
    except Exception as e:
        print('Failed to generate Demand table: ' + traceback.format_exc())
        return 1
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."DEMAND"')
            DEMAND.to_sql('demand', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Demand table succesfully uploaded to HANA.')
            # DEMAND.to_excel('Demand.xlsx', index = False)
        except Exception as e:
            print('Failed to upload Demand table to HANA: ' + traceback.format_exc())
            return 1
        try:
            connection_to_HANA.execute('DELETE FROM "SAC_OUTPUT"."ERROR_DEMAND"')
            ERROR_DEMAND.to_sql('error_demand', schema = 'sac_output', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Error demand table succesfully uploaded to HANA.')
            #ERROR_DEMAND.to_excel('ERROR_DEMAND.xlsx', index = False)
        except Exception as e:
            print('Failed to upload Error demand table to HANA: ' + traceback.format_exc())
            return 1

    #4) Inventory bulk (must be created after Demand in order to validate)
    try:
        INVENTORY_BULK = generate_inventory_bulk(Inventory, ItemMaster, Facility, Model_WorkCenters, DEMAND, BREAKOUT)
        print('Bulk inventory table succesfully generated.')
    except Exception as e:
        print('Failed to generate Inventory bulk table: ' + traceback.format_exc())
        return 1
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."BULK_INVENTORY"')
            #INVENTORY_BULK.to_excel('Inventory_bulk.xlsx', index = False)
            INVENTORY_BULK.to_sql('bulk_inventory', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Bulk inventory table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Bulk inventory table to HANA: ' + traceback.format_exc())
            return 1
    
    #5) WO Demand for SAC
    try:
        WO_DEMAND = generate_wo_demand(ItemMaster, WorkOrders)
    except Exception as e:
        print('Failed to generate WO Demand table: ' + traceback.format_exc())
        return 1
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "SAC_OUTPUT"."WO_DEMAND" WHERE "Process Date" = \'%s\' and "Run" = \'1\'' % datetime.date.today().strftime("%Y-%m-%d"))
            WO_DEMAND.to_sql('wo_demand', schema = 'sac_output', con = connection_to_HANA, if_exists = 'append', index = False)
        except Exception as e:
            print('Failed to upload WO Demand table to HANA: ' + traceback.format_exc())
            return 1
    
    #6) Save upload time info
    try:
        connection_to_HANA.execute('DELETE FROM "SAGE"."LOG"')
        time = pd.to_datetime(datetime.datetime.now())
        total_demand = DEMAND['Demand quantity (pounds)'].sum()
        pd.DataFrame({'TIME': [time], 'TOTAL_DEMAND': [total_demand]}).to_sql('log', schema = 'sage', con = connection_to_HANA, if_exists = 'append', index = False)
        print('Updated backup time successfully.')
        display_info_widget['state'] = 'normal'
        display_info_widget.insert('end', f'Time of cloud database last update from REST services: {time}\n\nTotal demand quantity: {total_demand}\n\n')
        display_info_widget['state'] = 'disabled'
    except Exception as e:
        print('Failed to update backup time: ' + traceback.format_exc())

    if to_excel:
        dic = {'BREAKOUT': 'Breakout_file.xlsx',
            'PACKLINES': 'Packlines.xlsx',
            'EXTRUDERS': 'Extruders.xlsx'}
        for table in dic:
            try:
                locals()[table].to_excel(dic[table], index = False)
                print(f'{table} saved to Excel.')
            except:
                print('Couldn\'t save table.')
        EXTRUDERS_SCHEDULE = pd.read_sql_table('extruders_schedule', schema = 'manual_files', con = connection_to_HANA)
        try:
            with pd.ExcelWriter('Demand.xlsx') as writer:
                DEMAND.to_excel(writer, sheet_name = 'Demand', index = False)
                INVENTORY_BULK.to_excel(writer, sheet_name = 'Bulk Inventory', index = False)
                EXTRUDERS_SCHEDULE.to_excel(writer, sheet_name = 'Extruders Schedule', index = False)
            print('Demand saved to Excel.')
        except:
            print('Couldn\'t save to Excel.')

    display_info(DEMAND, ERROR_DEMAND)

    return 0

def display_info(DEMAND, ERROR_DEMAND):
    df = DEMAND.copy()
    df['Due date'] = pd.to_datetime(df['Due date'])
    df = df[['Due date', 'Demand quantity (pounds)', 'Facility']].groupby(['Due date', 'Facility']).sum()
    df = df.unstack(fill_value = 0)
    df = df.droplevel(0, axis = 1)
    df['Week start'] = df.index.map(lambda x: x - datetime.timedelta(x.weekday()))
    df['Week start'] = df.index.map(lambda x: x - datetime.timedelta(x.weekday()))
    df = df.groupby('Week start').sum()

    a = df.cumsum().plot(kind = 'area', ax = ax)
    a.ticklabel_format(axis = 'y', style = 'sci', scilimits = (6,6))
    a.set_ylabel('Pounds (in millions)')

    fig.canvas.draw_idle()
    # canvas.get_tk_widget().pack(anchor = 'w')
    display_info_widget.window_create('end', window = canvas.get_tk_widget())
    
    error_demand_pt = CustomTable(error_demand_lf, dataframe = ERROR_DEMAND, showtoolbar = False, showstatusbar = False, editable = False)
    error_demand_pt.adjustColumnWidths()
    error_demand_pt.show()
    # error_demand_pt.draw_idle() #TODO no funciona

def update_db_from_SAGE_command():
    loading_window_from_SAGE = LoadingWindow(root, update_db_from_SAGE)

def generate_model_files_from_backup_command():
    loading_window_backup = LoadingWindow(root, generate_model_files_from_backup)

    #TODO terminar esta función
def add_manual_input():
    manual_window = ManualInput(root, connection_to_HANA, 
                    extruders_schedule = 'Extruders Schedule', families = 'Families', product_priority = 'Product Priority', customer_priority = 'Customer Priority', )
    #manual_window.resizable(0,0)

def run_experiment(experiment):
    subprocess.run(f'Model\CJFoods_windows-{experiment}.bat')

def startup():
    out_string = connectToHANA()
    statusbar.config(text = out_string)
    if connection_to_HANA:
        add_manual_input_btn['state'] = 'normal'
        (time, total_demand) = connection_to_HANA.execute('SELECT * FROM "SAGE"."LOG"').first()
        display_info_widget['state'] = 'normal'
        display_info_widget.insert('end', f'Time of cloud database last update from REST services: {time}\n\nTotal demand quantity: {total_demand}\n\n')
        display_info_widget['state'] = 'disabled'
        # #Load manual tables
        # #TODO clean hardcoding        -----------
        # global extruders_schedule_df, families_df, product_priority_df, customer_df
        # extruders_schedule_df = pd.read_sql_table('extruders_schedule', schema = 'manual_files', con = connection_to_HANA)
        # families_df = pd.read_sql_table('families', schema = 'manual_files', con = connection_to_HANA)
        # product_priority_df = pd.read_sql_table('product_priority', schema = 'manual_files', con = connection_to_HANA)
        # customer_df = pd.read_sql_table('customer_priority', schema = 'manual_files', con = connection_to_HANA)
        # #--------------------------------------

if __name__ == '__main__':

    root = tk.Tk()
    root.title('ITE Consult Launcher')
    root.iconbitmap(default = 'iteIcon.ico')
    
    root.configure(bg = 'white')
    s = ttk.Style()
    root.state("zoomed")
    s_width = root.winfo_screenwidth()
    s_height = root.winfo_screenheight()
    #root.resizable(height = False, width = False)

    s = ttk.Style()
    s.configure('TFrame', background = 'white')
    s.configure('TLabelframe', background = 'white')
    s.configure('TLabelframe.Label', background = 'white')
    s.configure('TLabel', background = 'white')
    s.configure('TLoadingWindow.TFrame', background = 'grey')

    statusbar = tk.Label(root, text = 'Establishing connection to cloud database...', relief = tk.SUNKEN, anchor = 'w')
    statusbar.pack(side = tk.BOTTOM, fill = tk.X)

    buttons_frame = ttk.Frame(root, width = 270)
    buttons_frame.pack(side = tk.LEFT, padx = 10, pady = 10, fill = tk.Y)
    buttons_frame.pack_propagate(0)
    
    logo_canvas = tk.Canvas(buttons_frame)

    read_data_lf = ttk.LabelFrame(buttons_frame, text = '1. Select Data Source')
    read_data_lf.pack(fill = tk.X, padx = 10, pady = 10)

    read_data_frame = ttk.Frame(read_data_lf)
    read_data_frame.pack()

    update_db_from_SAGE_btn = ttk.Button(read_data_frame, text = 'Read from REST Services', command = lambda: update_db_from_SAGE_command())
    update_db_from_SAGE_btn.pack(padx = 10, pady = (15, 17), ipadx = 10, ipady = 2, fill = tk.X)

    generate_model_files_from_backup_btn = ttk.Button(read_data_frame, text = 'Read from Cloud Database', command = lambda: generate_model_files_from_backup_command())
    generate_model_files_from_backup_btn.pack(padx = 10, pady = (0, 20), ipadx = 10, ipady = 2, fill = tk.X)

    manual_input_lf = ttk.LabelFrame(buttons_frame, text = '2. Add Manual Input (optional)')
    manual_input_lf.pack(fill = tk.X, padx = 10, pady = 10)

    add_manual_input_btn = ttk.Button(manual_input_lf, text = 'Edit Manual Tables', command = lambda: add_manual_input())
    add_manual_input_btn.pack(padx = 10, pady = (15, 20), ipadx = 10, ipady = 2)
    add_manual_input_btn.state(['disabled'])

    run_model_lf = ttk.LabelFrame(buttons_frame, text = '3. Select Experiment')
    run_model_lf.pack(fill = tk.X, padx = 10, pady = 10)

    run_model_frame = ttk.Frame(run_model_lf)
    run_model_frame.pack()

    run_simulation_btn = ttk.Button(run_model_frame, text = 'Run Simulation', command = lambda: threading.Thread(target = run_experiment, args = ('simulation',), daemon = True).start())
    run_simulation_btn.pack(padx = 10, pady = (15, 17), ipadx = 10, ipady = 2, fill = tk.X)

    run_optimization_btn = ttk.Button(run_model_frame, text = 'Run Optimization', command = lambda: threading.Thread(target = run_experiment, args = ('optimization',), daemon = True).start())
    run_optimization_btn.pack(padx = 10, pady = (0, 20), ipadx = 10, ipady = 2, fill = tk.X)

    sac_buttons_lf = ttk.LabelFrame(buttons_frame, text = '4. View Cloud Stories')
    sac_buttons_lf.pack(fill = tk.X, padx = 10, pady = 10)

    sac_buttons_frame = ttk.Frame(sac_buttons_lf)
    sac_buttons_frame.pack()

    report_catalog_btn = ttk.Button(sac_buttons_frame, text = 'Report Catalog',
                            command = lambda: webopen('https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=home;tab=catalog'))
    report_catalog_btn.pack(padx = 10, pady = (15, 17), ipadx = 10, ipady = 2, fill = tk.X)

    run_summary_btn = ttk.Button(sac_buttons_frame, text = 'Run Summary',
                            command = lambda: webopen('https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=4B636301B40D93B66DBA27FC1BF0C2C9'))
    run_summary_btn.pack(padx = 10, pady = (0, 20), ipadx = 10, ipady = 2, fill = tk.X)

    demand_review_btn = ttk.Button(sac_buttons_frame, text = 'Demand Review',
                            command = lambda: webopen('https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=223A9B02F4538FFC82411EFAF07F6A1D'))
    demand_review_btn.pack(padx = 10, pady = (0, 20), ipadx = 10, ipady = 2, fill = tk.X)

    unassigned_wo_btn = ttk.Button(sac_buttons_frame, text = 'Schedule Review',
                            command = lambda: webopen('https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=E86A9B02F45046DC9A422670A0016DA9'))
    unassigned_wo_btn.pack(padx = 10, pady = (0, 20), ipadx = 10, ipady = 2, fill = tk.X)

    data_errors_btn = ttk.Button(sac_buttons_frame, text = 'Master Data Errors',
                            command = lambda: webopen('https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=315A9B02F45146C8478A9C88FAA53442'))
    data_errors_btn.pack(padx = 10, pady = (0, 20), ipadx = 10, ipady = 2, fill = tk.X)

    tables_separator = ttk.Separator(root, orient = 'vertical')
    tables_separator.pack(side = tk.LEFT, fill = tk.Y)

    right_frame = ttk.Frame(root)
    right_frame.pack(side = tk.LEFT, fill = tk.BOTH, expand = True)

    demand_info_lf = ttk.LabelFrame(right_frame, text = '    Demand Info')
    demand_info_lf.pack(fill = tk.X, padx = 15, pady = (15,0))

    display_info_frm = ttk.Frame(demand_info_lf)
    display_info_frm.pack(fill = tk.X, expand = True, padx = 0, pady = (15,0))

    display_info_widget = tk.Text(display_info_frm, wrap = 'word', state = 'disabled', relief = tk.FLAT, bg = 'white')
    display_info_widget.pack(fill = tk.X, expand = True, side = tk.LEFT, padx = (20, 0))
    display_info_ys = ttk.Scrollbar(display_info_frm, orient = 'vertical', command = display_info_widget.yview)
    display_info_ys.pack(side = tk.LEFT, fill = tk.Y)
    display_info_widget['yscrollcommand'] = display_info_ys.set

    error_demand_lf = ttk.LabelFrame(right_frame, text = '    Error Demand')
    error_demand_lf.pack(fill = tk.BOTH, expand = True, padx = 15, pady = 15)

    fig = Figure()
    ax = fig.add_subplot(111)
    canvas = FigureCanvasTkAgg(fig, master = display_info_widget)
    canvas.draw()

    threading.Thread(target = startup).start()

    root.mainloop()

if connection_to_HANA:
    connection_to_HANA.close()