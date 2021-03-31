#Buttons:
#*Update database from SAGE and generate model files
#*Generate model files
#*Run simulation
#*Run optimization
#*Save outputs

import subprocess
import tkinter as tk
from tkinter import ttk
import pandas as pd
import threading
import sqlalchemy
import datetime
import numpy as np
import pandastable

#This line prevents the bundled .exe from throwing a sqlalchemy related error
sqlalchemy.dialects.registry.register('hana', 'sqlalchemy_hana.dialect', 'HANAHDBCLIDialect')

#----

#toma la week de hoy y le suma 3, queda una mas de la que viene de JD Edwards

def week():
    date = datetime.date.today()
    year, week_num, day_of_week = date.isocalendar()
    week = 'Week ' + str(week_num + 3)
    return week


#creates extruders output for SAC

def extruders(schedule_bulk):
    
    #copy dataframe
    extruders = schedule_bulk.copy()

    #drop columns
    extruders.drop(['week', 
                    'waste', 
                    'entity', 
                    'seed', 
                    ], axis=1, inplace = True)
   
    #rename colums
    dictionary = {"original due date": "Original Due Date",
                  "ending hour": "Ending Hour",
                  "starting hour": "Starting Hour",
                  "extruder sequence": "Sequence",
                  "run":"Run", 
                  "machine" : "WorkCenter",
                  "starting": "Starting Date",
                  "ending": "Ending Date",
                  "prod planned": "Demand", 
                  "prod att": "Production",
                  "shrinkage": "Shrinkage",
                  "hours": "Hours",
                  "fg assigned": "ItemRef",
                  "last change over":"CO Time",
                  "stuck time" : "Waiting Time", 
                  "bulk code":"ItemNumber",
                  "purchase order": "Purchase Order",
                  "sales order":"Sales Order",
                  "work order":"Work Order Ref",
                  "machine code":"WorkCenter Code",
                  "facility":"Facility",
                  "facility code":"Facility Code"}
    extruders.rename(columns= dictionary ,inplace=True)
    
    #fill nan with 0
    extruders = extruders.fillna('0')

    #insert process date, categorycode
    extruders['Process Date'] = datetime.date.today().strftime("%Y-%m-%d")
    extruders['CategoryCode'] = "INT"
    
    #change data types to int
    extruders["Production"] = extruders["Production"].astype(float).astype(int)
    extruders["Demand"] = extruders["Demand"].astype(float).astype(int)
    
    #from str to floats
    extruders["Shrinkage"] = extruders["Shrinkage"].astype(float)
    extruders["CO Time"] = extruders["CO Time"].astype(float)
    extruders["Hours"] = extruders["Hours"].astype(float)
    extruders["Waiting Time"] = extruders["Waiting Time"].astype(float)

    #keep only dates of timestamp
    extruders["Ending Date"] = extruders["Ending Date"].str.split(" ", n = 1, expand = True)[0]
    extruders["Starting Date"] = extruders["Starting Date"].str.split(" ", n = 1, expand = True)[0]
    extruders["Original Due Date"] = extruders["Original Due Date"].str.split(" ", n = 1, expand = True)[0]

    #round 
    extruders = extruders.round(1)
    
    #replace missing with 0
    extruders = extruders.replace('missing','0')
    
    #return dataframe
    return extruders




#inventory

def inventory(bulk_inventory, extruders_df):
    
    #copy df
    bulk_inventory_copy = bulk_inventory.copy()
    
    #rename columns
    bulk_inventory_copy.rename(columns = {
                                    'Component formula':'ItemNumber',
                                    'Facility':'Facility Code',
                                    'Quantity': 'Inventory'},
                                    inplace = True)
    
    #process date y run
    bulk_inventory_copy['Process Date'] = datetime.date.today().strftime("%Y-%m-%d")
    bulk_inventory_copy['Run'] = extruders_df.loc[0,"Run"]
    
    bulk_inventory_copy['Facility'] = bulk_inventory_copy['Facility Code'].map({
                                    '20001':'Bern', 
                                    '20005':'PC10', 
                                    '20006':'BAXTER SP',
                                    '20005':'PC30'})
    
    #fill null values
    bulk_inventory_copy = bulk_inventory_copy.fillna('0')

    #data type
    bulk_inventory_copy["Inventory"] = bulk_inventory_copy["Inventory"].astype(float).astype(int)

    
    #category code
    bulk_inventory_copy['CategoryCode'] = "Inventory"
    
    #replace missing with 0
    bulk_inventory_copy = bulk_inventory_copy.replace('missing','0')

    #return
    return bulk_inventory_copy




#creates packline output for SAC

def packlines(schedule_sku, extruders_df):
        
    #create a copy of the df
    packlines = schedule_sku.copy()
    

    
    # drop columns
    packlines.drop(['order demand pounds',
                    'production demand bags',
                    'sku description',
                    'inventory demand bags', 
                    'entity', 
                    'seed'
                   ], axis=1, inplace = True)

    # rename columns
    dictionary = {"ending hour": "Ending Hour", 
                  "starting hour": "Starting Hour", 
                  "starting date": "Starting Date", 
                  "ending date": "Ending Date", 
                  "original due date": "Original Due Date",
                  "machines sequences":"Sequence",
                  "run": "Run", 
                  "sku": "ItemNumber", 
                  "formula": "Diet", 
                  "machine": "WorkCenter", 
                  "production demand pounds": "Demand", 
                  "demand attained pounds": "Production", 
                  "order demand bags": "Bags Demand", 
                  "demand attained bags": "Bags Production", 
                  "hours": "Hours",
                  "purchase order": "Purchase Order",
                  "sales order":"Sales Order",
                  "work order":"Work Order",
                  "inventory demand pounds": "Inventory",
                  "machine code":"WorkCenter Code",
                  "facility":"Facility",
                  "facility code":"Facility Code"}
                  
    packlines.rename(columns = dictionary, inplace=True)
    
    #fill nan with 0
    packlines = packlines.fillna('0')
    
    #change data type to int
    packlines["Bags Production"] = packlines["Bags Production"].astype(float).astype(int)
    packlines["Bags Demand"] = packlines["Bags Demand"].astype(float).astype(int)
    packlines["Demand"] = packlines["Demand"].astype(float).astype(int)
    packlines["Production"] = packlines["Production"].astype(float).astype(int)
    packlines["Inventory"] = packlines["Inventory"].astype(float).astype(int)

    #change to float
    packlines["Hours"] = packlines["Hours"].astype(float)
    
    #round floats
    packlines = packlines.round(1)
    
    #insert version, entity, process date, CategoryCode
    packlines["Process Date"] = datetime.date.today().strftime("%Y-%m-%d")
    packlines['CategoryCode'] = "FG"

    #keep only dates of timestamp
    packlines["Starting Date"] = packlines["Starting Date"].str.split(" ", n = 1, expand = True)[0]
    packlines["Ending Date"] = packlines["Ending Date"].str.split(" ", n = 1, expand = True)[0]
    packlines["Original Due Date"] = packlines["Original Due Date"].str.split(" ", n = 1, expand = True)[0]
    
    #replace missing with 0
    packlines = packlines.replace('missing','0')
    
    #add run to packlines
    packlines['Run'] = extruders_df.loc[0,"Run"]

    #return dataframe
    return packlines




#unpack for sac

def unpacked(out_due_date_backlog, extruders_df):    
    
    #copy table
    out_due_date_backlog_copy = out_due_date_backlog.copy()

    #fill nan
    out_due_date_backlog_copy.fillna('0', inplace=True)
    
    #insert version, date, week, run
    out_due_date_backlog_copy['Process Date'] = datetime.date.today().strftime("%Y-%m-%d")
    out_due_date_backlog_copy['run'] = extruders_df.loc[0,"Run"]
    
    #rename
    out_due_date_backlog_copy.rename(columns={
            "run": "Run", 
            'finished good': 'ItemNumber', 
            'due date':'Original Due Date',
            'location': 'Facility',
            'amount unpacked': 'Unpacked Amount',
            'work order':'Work Order',
            'purchase order':'Purchase Order'
            }, inplace=True)

    #category code for unpacked
    out_due_date_backlog_copy['CategoryCode'] = "Unpacked"

    #attribute name
    out_due_date_backlog_copy.name = 'UNPACKED_SAC'
    
    #return unpack
    return out_due_date_backlog_copy




#create unified table of packlines and extrusion that will be uploaded to Hana

def unified_sac(packlines_df, extruders_df, inventory_df, unpacked_df):
    
    #append one to the other
    unified_table = extruders_df.append([packlines_df, 
                                     inventory_df, 
                                     unpacked_df], 
                                     ignore_index=True)

    #fill nulls with 0
    unified_table.fillna('0', inplace=True)
    
    #drop row if itemnumber is 0
    unified_table = unified_table[unified_table.ItemNumber != '0']
    
    #replace missing with 0
    unified_table = unified_table.replace('missing','0')
    
    #name attribute
    unified_table.name = 'UNIFIED_SAC'

    #return unified table
    return unified_table




#create WO demand planning of SAGE to create the unassigned workorders

def wo_demand(itemmaster,workorders, extruders_df):
    
    #create a copy
    ItemMaster_copy = itemmaster.copy()
    WorkOrders_copy = workorders.copy()
    
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
                                                   ]], 
                                                  on='ItemNumber',
                                                  how = 'inner')
    
    
    #filter based on two conditions
    merge = merge[
                  ((merge["CategoryCode"]=="INT") & (merge["Operation"]=='20')) | 
                  ((merge["CategoryCode"]=='FG') & (merge["Operation"]=='10'))
                  ]
    
    
    #planned - complete
    merge['Demand'] = merge['PlannedQty'].astype(int) - merge['CompletedQty'].astype(int)

    #multiply weight by plannedqty for FG
    merge.loc[merge['CategoryCode'] == 'FG', 'Demand'] = merge['Demand'].astype(int) * merge['ItemWeight'].astype(float)
        
    #change data type
    merge['Demand'] = merge['Demand'].astype(int)
    
    # 0 for negative numbers
    merge.loc[merge['Demand'] < 0, 'Demand'] = 0 
    
    #fill null values with 0
    merge.fillna('0', inplace=True)
    
    #run and process date
    merge['Run'] = extruders_df.loc[0,"Run"]
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




#grouped itemnumbers per uninterrupted sequence and workcenter
def group_extruders(extruders_df, inventory_df):

    #create copy
    group_extruders = extruders_df.copy()
    inventory_copy = inventory_df.copy()

    #inventory merge
    group_extruders = group_extruders.merge(inventory_copy[['ItemNumber', 'Inventory']], 
                            on='ItemNumber', 
                            how = 'left')
    
    #drop columns
    group_extruders.drop(['Shrinkage', 'CO Time','Waiting Time'], axis=1, inplace = True)

    #change sequence data type and sort the df by WC and sequence
    group_extruders['Sequence'] = group_extruders['Sequence'].astype(int)
    group_extruders = group_extruders.sort_values(['WorkCenter','Sequence'])
    group_extruders['Sequence'] = group_extruders['Sequence'].astype(str)

    #keep first colum of group to keep start date
    first = group_extruders[['ItemNumber',
                             'Sequence', 
                             'Purchase Order',
                             'Starting Date', 
                             'Starting Hour',
                             'Run',
                             'Process Date',
                             'WorkCenter',
                             'Facility',
                             'WorkCenter Code',
                             'Facility Code',
                             'Inventory',
                             'Work Order Ref'
                            ]].groupby(by=[(
                            group_extruders.ItemNumber!=group_extruders.ItemNumber.shift()).cumsum()
                            ], 
                            as_index=False).nth([0]).reset_index(drop=True)

    
    #keep last row of group to keep end date
    last = group_extruders[['ItemNumber', 
                            'Ending Date', 
                            'Ending Hour'
                           ]].groupby(by=[(
                            group_extruders.ItemNumber!=group_extruders.ItemNumber.shift()).cumsum()
                            ], as_index=False).nth([-1]).reset_index(drop=True)
    
    #create index as column
    first['index'] = first.index
    last['index'] = last.index
    
    #merge first and last rows of group to have the first and last date on same row
    firstlast = pd.merge(first, last, 
                             how='inner', 
                             on=['ItemNumber', 
                                 'index'])
    
    
    #drop inventory to not sum
    group_extruders.drop('Inventory', inplace = True, axis = 1)

    #sum all measures of group
    suma = group_extruders.groupby(by=[(
                            group_extruders.ItemNumber!=group_extruders.ItemNumber.shift()).cumsum(),
                            'ItemNumber'
                            ], as_index=False).sum()

    #create index as column
    suma['index'] = suma.index

    #merge measures with last and first rows
    merge = pd.merge(firstlast, suma, 
                             how='inner',
                             on=['ItemNumber', 
                                 'index'])
    
    #fill null with 0
    merge.fillna('0', inplace=True)

    #round decimals
    merge = merge.round(1)
    
    #drop index column
    merge.drop(['index'], axis=1, inplace = True)
    
    #return df
    return merge





#assigned wo to group extrusion

def assigned_wo(group_extruders_df, wo_demand_df):
    
    #create a copu
    group_extruders_copy = group_extruders_df.copy()
    wo_bulk_copy = wo_demand_df.copy()
    
    #filter demand per categorycode = INT
    filter2 = wo_bulk_copy['CategoryCode'] == 'INT'
    wo_bulk_copy = wo_bulk_copy[filter2]

    #sort key values
    group_extruders_copy = group_extruders_copy.sort_values('Production')
    wo_bulk_copy = wo_bulk_copy.sort_values('Demand')
    
    #drop demand from grouped
    group_extruders_copy.drop(['Demand'], inplace = True, axis = 1)
    
    #merge as of
    merge = pd.merge_asof(group_extruders_copy, 
                          wo_bulk_copy[[
                            'Demand', 
                            'ItemNumber', 
                            'WorkOrderNumber']], 
                              left_on ="Production", 
                              right_on = 'Demand', 
                              direction = 'nearest',
                              by = 'ItemNumber')
    
    #create boolean threshold on dummy
    merge['dummy'] = (merge['Production'] / merge['Demand']) * 100
    merge['dummy'] = merge['dummy'].between(95, 105)
    
    #conditional threshold to set values
    merge['Close/Distant Assigned'] = merge['dummy'].map({True: 'Close', False: 'Distant'})
        
    #drop dummy
    merge.drop(['dummy'], axis=1, inplace = True)
    
    #rename
    merge.rename({'WorkOrderNumber':'WorkOrder Assigned'}, inplace = True, axis = 1)
    
    #create time column
    merge['Starting Time'] = merge['Starting Date'] + ' ' + merge['Starting Hour']
    merge['Ending Time'] = merge['Ending Date'] + ' ' + merge['Ending Hour']
    
    #fill nulls with 0
    merge.fillna('0', inplace=True)
    
    #drop if itemnumber ==0
    merge = merge[merge.ItemNumber != '0']

    #attribute name
    merge.name = "GROUPED_EXTRUDERS_ASSIGNED_SAC"
    
    #return merge
    return merge


#sube la lista de tablas de SACA a Hana y pisa segun week y run

def upload_output_to_hana():
    
    #coneccion a variable
    connectToHANA()
    
    #Hana output
    out_due_date_backlog = pd.read_sql('SELECT * FROM "OUTPUT"."OUT_DUE_DATE_BACKLOG"', con=connection_to_HANA)
    schedule_bulk = pd.read_sql('SELECT * FROM "OUTPUT"."SCHEDULE_BULK"', con=connection_to_HANA)
    schedule_sku = pd.read_sql('SELECT * FROM "OUTPUT"."SCHEDULE_SKU"', con=connection_to_HANA)

    #sage Hana
    itemmaster = pd.read_sql('SELECT * FROM "SAGE"."ITEMMASTER"', con=connection_to_HANA)
    workorders = pd.read_sql('SELECT * FROM "SAGE"."WORKORDERS"', con=connection_to_HANA)

    #anylogic
    bulk_inventory = pd.read_sql('SELECT * FROM "ANYLOGIC"."BULK_INVENTORY"', con=connection_to_HANA)

    # variables for unified df 

    extruders_df = extruders(schedule_bulk)
    packlines_df = packlines(schedule_sku, extruders_df)
    unpacked_df = unpacked(out_due_date_backlog, extruders_df)
    inventory_df = inventory(bulk_inventory, extruders_df)
    wo_demand_df = wo_demand(itemmaster, workorders, extruders_df)
    unified_sac_df = unified_sac(packlines_df, extruders_df, inventory_df, unpacked_df)

    #variables for assigned wo

    group_extruders_df = group_extruders(extruders_df, inventory_df)
    assigned_wo_df = assigned_wo(group_extruders_df, wo_demand_df)

    lista_tablas_para_SAC = [assigned_wo_df, unified_sac_df, wo_demand_df]
    
    #itera sobre las tablas, pisa segun run y process date. Si no funciona, dale error
    for table in lista_tablas_para_SAC:
        try:

            #variables de run y process date
            Run = table.loc[1,"Run"]
            Process_Date = table.loc[1,"Process Date"]

            #execute sql to delete rows on database based on run and process date
            connection_to_HANA.execute(f"""DELETE FROM "SAC_OUTPUT".{table.name} WHERE "Process Date" = '{Process_Date}' and "Run" = '{Run}'""")
            print('Values deleted succesfully')

            #append dataframe to the table
            table.to_sql(table.name.lower(), schema='SAC_OUTPUT', con=connection_to_HANA, if_exists='append', index=False)
            print(table.name + ' uploaded succesfully')

        except Exception as e:

            #print problems
            print(table.name +' failed to upload! ' + str(e))

#-----

connection_to_HANA = None

def connectToHANA():
    global connection_to_HANA
    if not connection_to_HANA:
        try:
            connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
            print('Connection established.')
        except Exception as e:
            print('Connection failed! ' + str(e))

def generate_breakout_file(BOM, ItemMaster, Facility, MD_Bulk_Code, Finished_Good):
    #Merging with ItemMaster
    BREAKOUT = BOM[['ItemNumber', 'Facility', 'BomCode', 'ComponentItemNumber', 'Quantity']].merge(ItemMaster[['ItemNumber', 'CategoryCode', 'ProductType', 'DefaultFacility', 'ItemWeight', 'BagWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left', validate = 'm:1')
    BREAKOUT.dropna(subset = ['ItemWeight', 'BagWeight'], inplace = True)
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

def generate_demand(WorkOrders, ItemMaster, Model_WorkCenters, BREAKOUT, PACKLINES, EXTRUDERS):    
    #TODO PROVISORIO
    global NOT_IN_BREAKOUT
    global NOT_IN_EXTRUDERS
    global NOT_IN_PACKLINES
    #TODO subir tabla con FG excluidos a HANA
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
    NOT_IN_EXTRUDERS = CFS_IN_DEMAND[['Finished good', 'Component formula', 'in_extruders', 'Blend percentage', 'WorkOrderNumber', 'Demand quantity (pounds)']].merge(CFS_IN_DEMAND[['Finished good', 'in_extruders']].groupby('Finished good', as_index = False).all(), on = 'Finished good', how = 'left', suffixes = ['_cf', '_fg']).query('in_extruders_fg == False')
    NOT_IN_EXTRUDERS['Rejected Pounds'] = NOT_IN_EXTRUDERS['Demand quantity (pounds)'].astype(float) * NOT_IN_EXTRUDERS['Blend percentage'].astype(float)
    NOT_IN_EXTRUDERS['Cause'] = 'At least one component has no extrusion rate'
    NOT_IN_EXTRUDERS.rename({'Finished good': 'ItemNumber FG', 'Component formula': 'ItemNumber INT', 'WorkOrderNumber': 'Work Order'}, axis = 1, inplace = True)
    NOT_IN_EXTRUDERS = NOT_IN_EXTRUDERS[['ItemNumber FG', 'Work Order', 'Cause', 'ItemNumber INT', 'Rejected Pounds']]
    DEMAND = DEMAND.merge(CFS_IN_DEMAND[['Finished good', 'in_extruders']].groupby('Finished good').all().query('in_extruders == True'), left_on = 'ItemNumber', right_on = 'Finished good', how = 'inner').drop('in_extruders', axis = 1)
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
    DEMAND['Process date'] = datetime.date.today().strftime("%Y-%m-%d")
    DEMAND.rename({'ItemNumber': 'Finished good', 'CustomerName': 'Customer', 'Model plant': 'Location', 'WorkCenter': 'Packline', 
                   'WorkOrderNumber': 'Work order'}, axis = 1, inplace = True)
    DEMAND = DEMAND[['Finished good', 'Description', 'Customer', 'Formula', 'Inventory', 'Priority product', 'Priority', 'Raw material date', 
                     'Demand quantity (pounds)', 'Due date', 'Location', 'Purchase order', 'Sales order', 'Original due date',
                     'Entity', 'Work order', 'Packline', 'Process date', 'Facility']]

    ERROR_DEMAND = pd.concat([NOT_IN_BREAKOUT, NOT_IN_EXTRUDERS, NOT_IN_PACKLINES], ignore_index = True)
    ERROR_DEMAND['Process Date'] = datetime.date.today().strftime("%Y-%m-%d")
    ERROR_DEMAND['Run'] = 1

    CUSTOMER_PRIORITY = pd.DataFrame(data = DEMAND['Customer'].unique(), columns = ['Customer'])
    CUSTOMER_PRIORITY['Priority'] = 0

    PRODUCT_PRIORITY = pd.DataFrame(data = DEMAND['Finished good'].unique(), columns = ['Finished good'])
    PRODUCT_PRIORITY['Priority'] = 0
    
    FAMILIES = pd.DataFrame(data = DEMAND['Finished good'].unique(), columns = ['Finished good'])
    FAMILIES['Family sequence'] = 0

    return DEMAND, ERROR_DEMAND, CUSTOMER_PRIORITY, PRODUCT_PRIORITY, FAMILIES

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
    
def generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters, Inventory, WorkCenters, MD_Bulk_Code, Finished_Good):

    #Model files generation and uploading

    #1) Breakout
    try:
        BREAKOUT = generate_breakout_file(BOM, ItemMaster, Facility, MD_Bulk_Code, Finished_Good)
        print('Breakout table succesfully generated.')
    except Exception as e:
        print('Failed to generate Breakout table: ' + str(e))
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."BREAKOUT_FILE"')
            #BREAKOUT.to_excel('Breakout_file.xlsx', index = False)
            BREAKOUT.to_sql('breakout_file', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Breakout table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Breakout table to HANA: ' + str(e))
                
    #2) Packlines and extruders
    try:
        PACKLINES, EXTRUDERS = generate_packlines_and_extruders(RoutingAndRates, ItemMaster, Model_WorkCenters, Facility, Finished_Good)
        print('Packlines and Extruders tables succesfully generated.')
    except Exception as e:
        print('Failed to generate Packlines and Extruders tables: ' + str(e))
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."PACKLINES"')
            #PACKLINES.to_excel('Packlines.xlsx', index = False)
            PACKLINES.to_sql('packlines', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Packlines table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Packlines table to HANA: ' + str(e))
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."EXTRUDERS"')
            #EXTRUDERS.to_excel('Extruders.xlsx', index = False)
            EXTRUDERS.to_sql('extruders', schema = 'anylogic', con = connection_to_HANA, if_exists ='append', index = False)
            print('Extruders table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Extruders table to HANA: ' + str(e))

    #3) Demand (must be created after Breakout, Packlines and Extruders in order to validate)
    try:
        DEMAND, ERROR_DEMAND, CUSTOMER_PRIORITY, PRODUCT_PRIORITY, FAMILIES = generate_demand(WorkOrders, ItemMaster, Model_WorkCenters, BREAKOUT, PACKLINES, EXTRUDERS)
        print('Demand table succesfully generated.')
    except Exception as e:
        print('Failed to generate Demand table: ' + str(e))
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."DEMAND"')
            DEMAND.to_sql('demand', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Demand table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Demand table to HANA: ' + str(e))
        try:
            #TODO upload ERROR_DEMAND to HANA
            connection_to_HANA.execute('DELETE FROM "SAC_OUTPUT"."ERROR_DEMAND"')
            ERROR_DEMAND.to_sql('error_demand', schema = 'sac_output', con = connection_to_HANA, if_exists = 'append', index = False)
        except Exception as e:
            print('Failed to upload Error demand table to HANA: ' + str(e))

    #4) Inventory bulk (must be created after Demand in order to validate)
    try:
        INVENTORY_BULK = generate_inventory_bulk(Inventory, ItemMaster, Facility, Model_WorkCenters, DEMAND, BREAKOUT)
        print('Bulk inventory table succesfully generated.')
    except Exception as e:
        print('Failed to generate Inventory bulk table: ' + str(e))
    else:
        try:
            connection_to_HANA.execute('DELETE FROM "ANYLOGIC"."BULK_INVENTORY"')
            #INVENTORY_BULK.to_excel('Inventory_bulk.xlsx', index = False)
            INVENTORY_BULK.to_sql('bulk_inventory', schema = 'anylogic', con = connection_to_HANA, if_exists = 'append', index = False)
            print('Bulk inventory table succesfully uploaded to HANA.')
        except Exception as e:
            print('Failed to upload Bulk inventory table to HANA: ' + str(e))


def update_db_from_SAGE():
    print('update_db_from_SAGE function called.')
    
    #Connect to HANA
    try:
        connectToHANA()
    except:
        print('Couldn\'t connect to database!') #TODO add warning message
        return

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
            connection_to_HANA.execute(f'DELETE FROM "SAGE".{table}')
            globals()[table].to_sql(table.lower(), con = connection_to_HANA, if_exists = 'append', index = False, schema = 'sage')
            print(f'Table {table} was uploaded to HANA succesfully.')
        except Exception as e:
            print(f'Couldn\'t save {table} table into HANA. ' + str(e))

    #Read manual files from HANA
    manual_files = ['Model_WorkCenters', 'MD_Bulk_Code', 'Finished_Good']

    for table in manual_files:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'manual_files', con = connection_to_HANA).astype(str)
            print(f'Table {table} succesfully read from HANA.')
        except Exception as e:
            print('Couldn\'t read table {table} from HANA. ' + str(e))

    generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters, Inventory, WorkCenters, MD_Bulk_Code, Finished_Good)
    
    print('generate_model_files_from_backup function finished.')

def generate_model_files_from_backup():
    print('generate_model_files_from_backup function called.')

    #Connect to HANA
    try:
        connectToHANA()
    except:
        print('Couldn\'t connect to database!') #TODO add warning message
        return

    tables = ['BOM', 'Inventory', 'Facility', 'ItemMaster', 'RoutingAndRates', 'WorkCenters', 'WorkOrders']

    #Read SAGE tables from HANA
    for table in tables:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'sage', con = connection_to_HANA).astype(str)
            print(f'Table {table} succesfully read from HANA.')
        except Exception as e:
            print(f'Couldn\'t read table {table} from HANA. ' + str(e))

    #Read manual files from HANA
    manual_files = ['Model_WorkCenters', 'MD_Bulk_Code', 'Finished_Good']
    
    for table in manual_files:
        try:
            globals()[table] = pd.read_sql_table(table.lower(), schema = 'manual_files', con = connection_to_HANA).astype(str)
            print(f'Table {table} sucesfully read from HANA.')
        except:
            print(f'Couldn\'t read {table} from HANA.' + str(e))

    generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters, Inventory, WorkCenters, MD_Bulk_Code, Finished_Good)
    
    print('generate_model_files_from_backup function finished.')

def update_db_from_SAGE_command():
    update_db_from_SAGE_thread = threading.Thread(target = update_db_from_SAGE, daemon = True)
    update_db_from_SAGE_thread.start()
    #update_db_from_SAGE_pgb.start()
    update_db_from_SAGE_thread.join()
    #update_db_from_SAGE_pgb.stop()

def generate_model_files_from_backup_command():
    generate_model_files_from_backup_thread = threading.Thread(target = generate_model_files_from_backup, daemon = True)
    generate_model_files_from_backup_thread.start()
    #generate_model_files_from_backup_pgb.start()
    loading_window = tk.Toplevel(root)
    loading_window.resizable(height = False, width = False)
    loading_window.grab_set()
    loading_frame = ttk.Frame(loading_window)
    loading_frame.pack()
    loading_label = ttk.Label(loading_frame, text = 'Loading...')
    loading_label.pack(pady = 10)
    cancel_btn = ttk.Button(loading_frame, text = 'Cancel')
    cancel_btn.pack()
    generate_model_files_from_backup_thread.join()
    loading_window.destroy()
    #generate_model_files_from_backup_pgb.stop()

def run_experiment(experiment):
    subprocess.run(f'Model\CJFoods_windows-{experiment}.bat')

def save_outputs_command():
    save_outputs_thread = threading.Thread(target = upload_output_to_hana, daemon = True)
    save_outputs_thread.start()
    save_outputs_pgb.start()
    save_outputs_thread.join()
    save_outputs_pgb.stop()

if __name__ == '__main__':
    root = tk.Tk()
    root.title('Alphia Launcher')
    root.configure(bg = 'white')
    s = ttk.Style()
    #root.state("zoomed")
    #root.resizable(height = False, width = False)
    
    s = ttk.Style()
    s.configure('TFrame', background = 'white')
    s.configure('TLabelframe', background = 'white')
    s.configure('TLabelframe.Label', background = 'white')

    buttons_frame = ttk.Frame(root)
    buttons_frame.pack(side = tk.LEFT, padx = 10, pady = 10, fill = tk.Y)

    tables_separator = ttk.Separator(root, orient = 'vertical')
    tables_separator.pack(side = tk.LEFT, fill = tk.Y)

    read_data_lf = ttk.LabelFrame(buttons_frame, text = '1. Select data source')
    read_data_lf.pack(fill = tk.X, ipadx = 10, ipady = 5, padx = 10, pady = 10)
    
    # source = tk.StringVar()
    # source.set('update_db_from_SAGE_command')
    
    # update_db_from_SAGE_rb = ttk.Radiobutton(read_data_lf, text = 'Read from SAGE', variable = source, value = 'update_db_from_SAGE_command')
    # update_db_from_SAGE_rb.pack(padx = 10, pady = 5, anchor = 'w')
    
    # generate_model_files_from_backup_rb = ttk.Radiobutton(read_data_lf, text = 'Read from HANA backup', variable = source, value = 'generate_model_files_from_backup_command')
    # generate_model_files_from_backup_rb.pack(padx = 10, pady = 5, anchor = 'w')
    
    # generate_model_files_btn = ttk.Button(read_data_lf, text = 'Read data', command = lambda: threading.Thread(target = eval(source.get()), daemon = True).start())
    # generate_model_files_btn.pack(padx = 15, pady = 10, anchor = 'e')

    update_db_from_SAGE_btn = ttk.Button(read_data_lf, text = 'Read from SAGE', command = lambda: threading.Thread(target = update_db_from_SAGE_command, daemon = True).start())
    update_db_from_SAGE_btn.pack(padx = 10, pady = 10)

    # update_db_from_SAGE_pgb = ttk.Progressbar(read_data_lf, mode = 'indeterminate')
    # update_db_from_SAGE_pgb.pack(padx = 10, pady = 10)

    generate_model_files_from_backup_btn = ttk.Button(read_data_lf, text = 'Read from HANA backup', command = lambda: threading.Thread(target = generate_model_files_from_backup_command, daemon = True).start())
    generate_model_files_from_backup_btn.pack(padx = 10, pady = 10)

    # generate_model_files_from_backup_pgb = ttk.Progressbar(read_data_lf, mode = 'indeterminate')
    # generate_model_files_from_backup_pgb.pack(padx = 10, pady = 10)

    manual_input_lf = ttk.LabelFrame(buttons_frame, text = '2. Add manual input (optional)')
    manual_input_lf.pack(fill = tk.X, padx = 10, pady = 10)
    
    add_manual_input_btn = ttk.Button(manual_input_lf, text = 'Edit manual tables')
    add_manual_input_btn.pack(padx = 10, pady = 10)
    add_manual_input_btn.state(['disabled'])

    run_model_lf = ttk.LabelFrame(buttons_frame, text = '3. Select experiment')
    run_model_lf.pack(fill = tk.X, padx = 10, pady = 10)

    run_simulation_btn = ttk.Button(run_model_lf, text = 'Run simulation', command = lambda: threading.Thread(target = run_experiment, args = ('simulation',), daemon = True).start())
    run_simulation_btn.pack(padx = 10, pady = 10)

    run_optimization_btn = ttk.Button(run_model_lf, text = 'Run optimization', command = lambda: threading.Thread(target = run_experiment, args = ('optimization',), daemon = True).start())
    run_optimization_btn.pack(padx = 10, pady = 10)

    save_outputs_btn = tk.Button(buttons_frame, text = 'Save outputs', command = lambda: threading.Thread(target = save_outputs_command, daemon = True).start())
    save_outputs_btn.pack(padx = 10, pady = 10)

    save_outputs_pgb = ttk.Progressbar(buttons_frame, mode = 'indeterminate')
    save_outputs_pgb.pack(padx = 10, pady = 10)

    show_tables_frame = ttk.Frame(root)
    show_tables_frame.pack(side = tk.LEFT, expand = True, fill = tk.BOTH)

    show_tables_notebook = ttk.Notebook(show_tables_frame)
    show_tables_notebook.pack(side = tk.LEFT, expand = True, fill = tk.BOTH, padx = 10, pady = 10)

    manual_tables_frame = ttk.Frame()
    show_tables_notebook.add(manual_tables_frame, text = '   Manual tables    ')
    manual_tables_pt = pandastable.Table(manual_tables_frame, dataframe = pd.DataFrame())
    manual_tables_pt.show()

    model_files_frame = ttk.Frame()
    show_tables_notebook.add(model_files_frame, text = '    Model tables    ')
    
    model_files_notebook = ttk.Notebook(model_files_frame)
    model_files_notebook.pack(expand = True, fill = tk.BOTH, padx = 10, pady = 10)
    
    breakout_fm = ttk.Frame()
    model_files_notebook.add(breakout_fm, text = 'Breakout')
    breakout_pt = pandastable.Table(breakout_fm, dataframe = pd.DataFrame())
    breakout_pt.show()

    root.mainloop()

if connection_to_HANA:
    connection_to_HANA.close()