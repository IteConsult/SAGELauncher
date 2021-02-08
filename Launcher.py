#Buttons:
#*Update database from SAGE
#*Run simulation
#*Run optimization
#*Save outputs

import subprocess
import tkinter as tk
import pandas as pd
import threading
import sqlalchemy
import sqlalchemy_hana

def connectToHANA():
    conn = None
    try:
        conn = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
        print('Connection established.')
    except Exception as e:
        print('Connection failed! ' + str(e))        

    return conn

def generate_breakout_file(BOM, ItemMaster, Facility):
    #Merging with ItemMaster to keep only 'FG' category items
    BREAKOUT = BOM[['ItemNumber', 'Facility', 'BomCode', 'ComponentItemNumber', 'Quantity']].merge(ItemMaster[['ItemNumber', 'CategoryCode']], on = 'ItemNumber', how = 'left')
    BREAKOUT = BREAKOUT.query('CategoryCode == "FG"').drop('CategoryCode', axis = 1)
    #Second merging with ItemMaster to keep only 'INT' category component items
    BREAKOUT = BREAKOUT.merge(ItemMaster[['ItemNumber', 'CategoryCode']], left_on = 'ComponentItemNumber', right_on = 'ItemNumber', suffixes = ['','_y']).drop('ItemNumber_y', axis = 1)
    BREAKOUT = BREAKOUT.query('CategoryCode == "INT"').drop('CategoryCode', axis = 1)
    #Blend percentage calculation
    BREAKOUT = BREAKOUT[['ItemNumber', 'Facility', 'BomCode', 'Quantity']].groupby(['ItemNumber', 'Facility', 'BomCode']).sum().rename({'Quantity': 'BlendPercentage'}, axis = 1).query('BlendPercentage != 0').merge(BREAKOUT, left_index = True, right_on = ['ItemNumber', 'Facility', 'BomCode'], how = 'left')
    BREAKOUT['BlendPercentage'] = BREAKOUT['Quantity']/BREAKOUT['BlendPercentage']
    BREAKOUT.drop('Quantity', axis = 1, inplace = True)
    #Buyable column addition (it depends on facility)
    BREAKOUT = BREAKOUT.merge(Facility[['ItemNumber', 'ItemFacility', 'Buyable']], left_on = ['ComponentItemNumber', 'Facility'], right_on = ['ItemNumber', 'ItemFacility'], how = 'left', suffixes = ['', '_y']).drop(['ItemNumber_y', 'ItemFacility'], axis = 1)
    #Set column order
    BREAKOUT = BREAKOUT[['ItemNumber', 'Facility', 'BomCode', 'ComponentItemNumber', 'BlendPercentage', 'Buyable']]
    #Change column names
    BREAKOUT.rename({'ItemNumber': 'Finished Good', 'ComponentItemNumber': 'Component Formula', 'BlendPercentage': 'Blend Percentage', 'ItemWeight': 'weight'}, axis = 1, inplace = True)    
    
    return BREAKOUT

def generate_packlines_and_extruders(RoutingAndRates, WorkCenters, ItemMaster):
    #Keep only those workcenters with UseStatus equal to 2 and are either packlines or extruders
    RATES = RoutingAndRates.query('UseStatus == 2').merge(WorkCenters[['WorkCenter', 'Area']], on = 'WorkCenter', how = 'left')
    packline_workcenter_filter = RATES['Area'] == "PACK"
    extruder_workcenter_filter = RATES['Area'] == 'EXTR'
    RATES = RATES[packline_workcenter_filter | extruder_workcenter_filter]
    #Rate calculation
    RATES = RATES.merge(ItemMaster[['ItemNumber', 'ItemWeight']], on = 'ItemNumber', how = 'left')
    RATES['OperationTimeUnits'] = RATES['OperationTimeUnits'].map({1: 1, 2: 60})
    RATES['Pounds/h'] = RATES['BaseQuantity']*RATES['ItemWeight']/(RATES['OperationTimeUnits']*RATES['OperationTime'])
    #Drop those for which the calculation failed (usually because weight is missing)
    RATES.dropna(subset=['Pounds/h'], inplace = True)
    #Drop columns that won't be needed anymore
    RATES.drop(['UseStatus', 'OperationNumber', 'OperationUOM', 'OperationTime', 'OperationTimeUnits', 'BaseQuantity', 'ItemWeight'], axis = 1, inplace = True)
    #Split the dataframe
    PACKLINES = RATES.query('Area == "PACK"').drop('Area', axis = 1)
    EXTRUDERS = RATES.query('Area == "EXTR"').drop('Area', axis = 1)
    
    return PACKLINES, EXTRUDERS

def update_db_from_SAGE(uploadToHana):
    
    #Connect to HANA
    connection = connectToHANA()

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
            locals()[table] = pd.read_json(table_urls[table], dtype = object)
            print(f'Table {table} succesfully loaded.')
        except Exception as e:
            print(f'Couldn\'t load table {table}: ' + str(e))
            #try to read backup from HANA?

    #Upload raw SAGE tables into HANA
    for table in table_urls:
        try:
            connection.execute(f'DELETE FROM {table}')
            locals()[table].to_sql(table.lower(), con = connection, if_exists = 'append', index = False)
            print(f'Table {table} was uploaded to HANA succesfully.')
        except Exception as e:
            print(f'Couldn\'t save {table} table into HANA. ' + str(e))
            
    #Model files generation and uploading
    #1) Breakout
    try:
        BREAKOUT = generate_breakout_file(BOM, ItemMaster, Facility)
        BREAKOUT.to_sql('breakout_file', con = connection, if_exists = 'replace', index = False)
        print('Breakout succesfully generated and uploaded to HANA.')
    except Exception as e:
        print('Failed to generate and upload Breakout table to HANA: ' + str(e))
    #2) Packlines and extruders
    try:
        current_table = 'Packlines'
        PACKLINES, EXTRUDERS = generate_packlines_and_extruders(RoutingAndRates, WorkCenters, ItemMaster)
        PACKLINES.to_sql('packlines', con = connection, if_exists = 'replace', index = False)
        current_table = 'Extruders'
        EXTRUDERS.to_sql('extruders', con = connection, if_exists ='replace', index = False)
    except Exception as e:
        print(f'Failed to generate and upload {current_table} table to HANA: ' + str(e))
    connection.close()

# def generate_model_tables():
#     tables = ['BOM', 'Inventory', 'Facility', 'ItemMaster', 'RoutingAndRates', 'WorkCenters', 'WorkOrders']
#     #Read tables from HANA
#     globals()[tables] = 
        
def run_experiment(experiment):
    subprocess.run(f'Model\CJFoods_windows-{experiment}.bat')
    
window = tk.Tk()
window.title('Alphia Launcher')
window.state("zoomed")

update_db_from_SAGE_btn = tk.Button(text = 'Update database from SAGE', command = lambda: threading.Thread(target = update_db_from_SAGE, args = (True,), daemon = True).start())
update_db_from_SAGE_btn.pack(pady = 10)

run_simulation_btn = tk.Button(text = 'Run simulation', command = lambda: threading.Thread(target = run_experiment, args = ('simulation',), daemon = True).start())
run_simulation_btn.pack(pady = 10)

run_optimization_btn = tk.Button(text = 'Run optimization', command = lambda: threading.Thread(target = run_experiment, args = ('optimization',), daemon = True).start())
run_optimization_btn.pack(pady = 10)

window.mainloop()
