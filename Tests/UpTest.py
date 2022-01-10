import pandas as pd
import sqlalchemy

BOM = pd.read_json(r'http://10.4.240.65/api/IntegrationAPI/GetBOM')

def connectToSQL():
    connection = None
    direccion_servidor = '10.4.240.65'
    nombre_bd = 'AnyLogic'
    nombre_usuario = 'AnyLogic'
    password = 'Axr24523'
    try:
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={direccion_servidor};DATABASE={nombre_bd};UID={nombre_usuario};PWD={password}'
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        connection = sqlalchemy.create_engine(connection_url).connect()
    except Exception as e:
        print('Could not establish connection. ' + str(e))
        tk.messagebox.showerror(title = 'Connection error', message = 'Could not establish connection.\n\n' + ''.join(traceback.format_exception_only(type(e), e)))
    return connection

with connectToSQL() as con:
    con.execute('DELETE FROM SAGE.BOM')
    BOM.to_sql('BOM', schema = 'SAGE', con = con, if_exists = 'append', index = False)