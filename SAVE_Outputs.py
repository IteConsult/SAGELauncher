import pandas as pd
import sqlalchemy
import sqlalchemy_hana
import sqlalchemy_hana.dialect
import datetime

sqlalchemy.dialects.registry.register('hana', 'sqlalchemy_hana.dialect', 'HANAHDBCLIDialect')

def connectToHANA():
    try:
        connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
        print('Connection established.')
    except Exception as e:
        print('Connection failed! ' + str(e))
        
    return connection_to_HANA

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
                                                   ]].groupby('ItemNumber').first(), 
                                                  on='ItemNumber',
                                                  how = 'left')
    
    
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
    merge['Demand'] = merge['Demand'].astype(int)
    
    # 0 for negative numbers
    merge.loc[merge['Demand'] < 0, 'Demand'] = 0
    
    #fill null values with 0
    merge.fillna('0', inplace=True)

    #Filter negative and 0 demand
    merge = merge[merge['Demand'] > 0]
    
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
    connection_to_HANA = connectToHANA()
    
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
    # extruders_df.to_excel('extruders.xlsx', index = False)
    packlines_df = packlines(schedule_sku, extruders_df)
    # packlines_df.to_excel('packlines.xlsx', index = False)
    unpacked_df = unpacked(out_due_date_backlog, extruders_df)
    # unpacked_df.to_excel('unpacked.xlsx', index = False)
    inventory_df = inventory(bulk_inventory, extruders_df)
    # inventory_df.to_excel('inventory.xlsx', index = False)
    wo_demand_df = wo_demand(itemmaster, workorders, extruders_df)
    # wo_demand_df.to_excel('wo_demand.xlsx', index = False)
    unified_sac_df = unified_sac(packlines_df, extruders_df, inventory_df, unpacked_df)
    # unified_sac_df.to_excel('unified_sac.xlsx', index = False)

    #variables for assigned wo

    group_extruders_df = group_extruders(extruders_df, inventory_df)
    # group_extruders_df.to_excel('group_extruders.xlsx', index = False)
    assigned_wo_df = assigned_wo(group_extruders_df, wo_demand_df)
    # assigned_wo_df.to_excel('assigned_wo.xlsx', index = False)

    lista_tablas_para_SAC = [assigned_wo_df, unified_sac_df, wo_demand_df]
    
    #itera sobre las tablas, pisa segun run y process date. Si no funciona, dale error
    for table in lista_tablas_para_SAC:
        try:

            #variables de run y process date
            # Run = table.loc[1,"Run"]
            Run = table["Run"].iloc[0]
            # Process_Date = table.loc[1,"Process Date"]
            Process_Date = table["Process Date"].iloc[0]

            #execute sql to delete rows on database based on run and process date
            connection_to_HANA.execute(f"""DELETE FROM "SAC_OUTPUT".{table.name} WHERE "Process Date" = '{Process_Date}' and "Run" = '{Run}'""")
            print('Values deleted succesfully')

            #append dataframe to the table
            table.to_sql(table.name.lower(), schema='SAC_OUTPUT', con=connection_to_HANA, if_exists='append', index=False)
            print(table.name + ' uploaded succesfully')

        except Exception as e:

            #print problems
            print(table.name +' failed to upload! ' + str(e))

if __name__ == '__main__':
    upload_output_to_hana()