import pandas as pd
import sys
import os

sys.path.insert(0, "C:\\Users\\admin\\Documents\\GitHub\\SAGELauncher")

from InputGeneration import AlphiaInputGenerator

class TestApp(AlphiaInputGenerator):
    def __init__(self):
        tables = ['BOM', 'Facility', 'Inventory', 'ItemMaster', 'RoutingAndRates', 'SalesOrders', 'WorkCenters', 'WorkOrders']
    
        for table in tables:
            setattr(self, table, pd.read_excel(f'Raw tables\\{table.lower()}.xlsx', dtype = str))
            print(f'{table} read.')
            
        manual_files = ['MD_Bulk_Code', 'Finished_Good', 'Families', 'Product_Priority', 'Customer_Priority', 'Model_WorkCenters', 'Model_WorkCenters_3']
            
        for table in manual_files:
            setattr(self, table, pd.read_excel(f'Manual Files\\{table}.xlsx', dtype = str))
            print(f'{table} read.')
            
if __name__ == '__main__':
    test = TestApp()
    BREAKOUT = test.generate_breakout_file()
    setattr(test, 'BREAKOUT', BREAKOUT)
    print('BREAKOUT generated.')
    PACKLINES, EXTRUDERS = test.generate_packlines_and_extruders()
    setattr(test, 'PACKLINES', PACKLINES)
    setattr(test, 'EXTRUDERS', EXTRUDERS)
    print('Packlines and extruders generated.')
    DEMAND, ERROR_DEMAND = test.generate_demand()
    setattr(test, 'DEMAND', DEMAND)
    setattr(test, 'ERROR_DEMAND', ERROR_DEMAND)
    print('Demand and error demand generated.')
    INVENTORY_BULK = test.generate_inventory_bulk()
    setattr(test, 'INVENTORY_BULK', INVENTORY_BULK)
    print('Inventory bulk generated.')
    
    tables = ['BREAKOUT', 'PACKLINES', 'EXTRUDERS', 'DEMAND', 'ERROR_DEMAND', 'INVENTORY_BULK']
    
    if not os.path.exists('Database input'):
        os.mkdir('Database input') 
    
    for table in tables:
        globals()[table].to_excel(f'Database input\\{table}.xlsx', index = False)