import pandas as pd
import traceback

class AlphiaInputGenerator():
    def __init__(self, app):
        self.app = app

    def update_db_from_SAGE(self):
        print('update_db_from_SAGE function called.')

        #Connect to HANA
        if not self.app.connection_to_HANA:
            print('Can\'t connect to database.')
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
            if self.app.connection_to_HANA:
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
                self.app.connection_to_HANA.execute(f'DELETE FROM "SAGE".{table}')
                globals()[table].to_sql(table.lower(), con = self.app.connection_to_HANA, if_exists = 'append', index = False, schema = 'sage')
                print(f'Table {table} was uploaded to HANA succesfully.')
            except Exception as e:
                print(f'Couldn\'t save {table} table into HANA. ' + traceback.format_exc())
                #loading_window.destroy()
                return 1

        #Read manual files from HANA
        manual_files = ['Model_WorkCenters', 'MD_Bulk_Code', 'Finished_Good', 'Product_Priority', 'Customer_Priority', 'Families']

        for table in manual_files:
            try:
                globals()[table] = pd.read_sql_table(table.lower(), schema = 'manual_files', con = self.app.connection_to_HANA).astype(str)
                print(f'Table {table} succesfully read from HANA.')
            except Exception as e:
                print('Couldn\'t read table {table} from HANA. ' + traceback.format_exc())
                #loading_window.destroy()
                return 1

        s_code = self.generate_and_upload_model_files(BOM, ItemMaster, Facility, RoutingAndRates, WorkOrders, Model_WorkCenters,
                                                    Inventory, WorkCenters, MD_Bulk_Code, Finished_Good, Families)

        print(f'generate_model_files_from_backup function finished with code {s_code}.')

        #loading_window.destroy()
        return s_code

    def generate_breakout_file(self, BOM, ItemMaster, Facility, MD_Bulk_Code, Finished_Good, Families):
        #Merging with ItemMaster
        BREAKOUT = BOM[['ItemNumber', 'Facility', 'BomCode', 'ComponentItemNumber', 'Quantity']].merge(ItemMaster[['ItemNumber', 'CategoryCode', 'ProductType', 'DefaultFacility', 'ItemWeight', 'BagWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left', validate = 'm:1')
        BREAKOUT.dropna(subset = ['ItemWeight', 'BagWeight'], inplace = True)
        #Keep FG items only
        BREAKOUT = BREAKOUT.query('CategoryCode == "FG"').drop('CategoryCode', axis = 1)
        #Try to keep BOM from DefaultFacility if possible
        AVAILABLE_PLANTS = BREAKOUT[['ItemNumber', 'Facility', 'DefaultFacility']].drop_duplicates()
        AVAILABLE_PLANTS['has_default_facility'] = np.where(AVAILABLE_PLANTS['Facility'] == AVAILABLE_PLANTS['DefaultFacility'], True, False)
        AVAILABLE_PLANTS = AVAILABLE_PLANTS.groupby('ItemNumber', as_index = False).agg({'Facility': 'first', 'DefaultFacility': 'first', 'has_default_facility': 'any'})
        AVAILABLE_PLANTS['BOMFacility'] = np.where(AVAILABLE_PLANTS['has_default_facility'], AVAILABLE_PLANTS['DefaultFacility'], AVAILABLE_PLANTS['Facility'])
        BREAKOUT = BREAKOUT.merge(AVAILABLE_PLANTS[['ItemNumber', 'BOMFacility']], on = 'ItemNumber', how = 'left')
        BREAKOUT = BREAKOUT.query('Facility == BOMFacility')
        #Filter Bomcodes according to Facility
        bomcode_table = {'20001': '20', '20006': '45', '20005': '40'}
        bom_filter = BREAKOUT['BomCode'] == BREAKOUT['Facility'].map(bomcode_table)
        BREAKOUT = BREAKOUT[bom_filter]
        #Second merging with ItemMaster to keep only 'INT' category component items
        BREAKOUT = BREAKOUT.merge(ItemMaster[['ItemNumber', 'CategoryCode']].groupby('ItemNumber').first(), left_on = 'ComponentItemNumber', right_on = 'ItemNumber', suffixes = ['','_y'])
        BREAKOUT = BREAKOUT.query('CategoryCode == "INT"').drop('CategoryCode', axis = 1)
        #Blend percentage calculation
        BREAKOUT['Quantity'] = BREAKOUT['Quantity'].astype(float)
        BREAKOUT = BREAKOUT[['ItemNumber', 'Facility', 'BomCode', 'Quantity']].groupby(['ItemNumber', 'Facility', 'BomCode']).sum().rename({'Quantity': 'BlendPercentage'}, axis = 1).query('BlendPercentage != 0').merge(BREAKOUT, left_index = True, right_on = ['ItemNumber', 'Facility', 'BomCode'], how = 'right')
        BREAKOUT['BlendPercentage'] = BREAKOUT['Quantity']/BREAKOUT['BlendPercentage']
        #Weight is BagSize
        BREAKOUT.rename({'BagWeight': 'Weight'}, axis = 1, inplace = True)
        BREAKOUT['Weight'] = BREAKOUT['Weight'].astype(float).round(2)
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

        return BREAKOUT

    def generate_packlines_and_extruders(self, RoutingAndRates, ItemMaster, Model_WorkCenters, Facility, Finished_Good):
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

    def generate_demand(self, WorkOrders, ItemMaster, Model_WorkCenters, Product_Priority, Customer_Priority, BREAKOUT, PACKLINES, EXTRUDERS):
        #TODO PROVISORIO
        DEMAND = WorkOrders.merge(ItemMaster[['ItemNumber', 'Description', 'CategoryCode', 'ItemWeight']].groupby('ItemNumber').first(), on = 'ItemNumber', how = 'left')
        #Filter only FG items
        DEMAND = DEMAND.query('CategoryCode == "FG"').copy()
        #Calculate demand in pounds and keep those with positive demand
        DEMAND = DEMAND.astype({'PlannedQty': float, 'CompletedQty': float, 'ItemWeight': float})
        DEMAND['Demand quantity (pounds)'] = ((DEMAND['PlannedQty'] - DEMAND['CompletedQty'])*DEMAND['ItemWeight']).round(0)
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

    def generate_inventory_bulk(self, Inventory, ItemMaster, Facility, Model_WorkCenters, DEMAND, BREAKOUT):
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
        
    def generate_wo_demand(self, ItemMaster, WorkOrders):
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

    def display_info(self, DEMAND, ERROR_DEMAND):
        df = DEMAND.copy()
        df['Due date'] = pd.to_datetime(df['Due date'])
        
        df = df[['Due date', 'Demand quantity (pounds)']]
        df['Week start'] = df['Due date'].map(lambda x: x - datetime.timedelta(x.weekday()))
        df = df.groupby('Week start', as_index = False).sum()

        plot = sns.barplot(x = "Week start", y = "Demand quantity (pounds)", data = df, 
                      estimator = sum, ci = None, ax = ax)
        app.ax.xaxis_date()
        x_dates = df['Week start'].dt.strftime('%Y-%m-%d')
        app.ax.set_xticklabels(labels=x_dates, rotation=45, ha='right')

        app.fig.canvas.draw_idle()
        # canvas.get_tk_widget().pack(anchor = 'w')
        app.display_info_widget.window_create('end', window = canvas.get_tk_widget())
        app.display_info_widget.insert('end', '\n\n\n')

        error_demand_pt = CustomTable(app.error_demand_frm, dataframe = ERROR_DEMAND, showtoolbar = False, showstatusbar = False, editable = False)
        error_demand_pt.adjustColumnWidths()
        error_demand_pt.show()