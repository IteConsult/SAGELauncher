#Standard libraries imports
import tkinter as tk
from tkinter import ttk
import time as time_module
import datetime
import sys
import os
import threading
import traceback
import subprocess
import queue
import collections

#Third party libraries
import sqlalchemy
from sqlalchemy.engine import URL
# from sqlalchemy_hana import dialect
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)
import pandas as pd
import seaborn as sns

#Local imports
from LauncherClass.Launcher import Launcher, LoadingWindow
from LauncherClass.CustomTable import CustomTable
from LauncherClass.ManualInput import ManualInput
from InputGeneration import * #TODO list functions

#This line prevents the bundled .exe from throwing a sqlalchemy-related error
# sqlalchemy.dialects.registry.register('hana', 'sqlalchemy_hana.dialect', 'HANAHDBCLIDialect')

app = Launcher('DETAILED SCHEDULING OPTIMIZATION')
app.root.state('zoomed')
app.root.minsize(1520, 700)

if len(sys.argv) > 1 and sys.argv[1] == 'd':
    debug = True
else:
    debug = False

# def connectToSQL():
    # connection = None
    # try:
        # connection = sqlalchemy.create_engine('hana://DBADMIN:BISjan2021*@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
    # except Exception as e:
        # print('Could not establish connection. ' + str(e))
        # tk.messagebox.showerror(title = 'Connection error', message = 'Could not establish connection.\n\n' + ''.join(traceback.format_exception_only(type(e), e)))
    # return connection

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

app.connectToSQL = connectToSQL

def add_manual_input(app):
    app.lw = LoadingWindow(app)
    app.manual_window = ManualInput(app)
    load_tables_thread = threading.Thread(target = load_tables, args = (app,), daemon = True)
    load_tables_thread.start()
    app.root.after(0, app.lw.check, load_tables_thread, app.manual_window.show)
    
def load_tables(app):
    try:
        with app.connectToSQL() as connection:
            app.q.append('Loading Extruders Schedule')
            app.manual_window.add_table('Extruders Schedule', 'extruders_schedule', connection)
            app.q.append('Loading Families')
            app.manual_window.add_table('Families', 'families', connection)
            app.q.append('Loading Product Priority')
            app.manual_window.add_table('Product Priority', 'product_priority', connection)
            app.q.append('Loading Customer Priority')
            app.manual_window.add_table('Customer Priority', 'customer_priority', connection)
    except Exception as e:
        app.register_error('Couldn\'t load tables.', e)

def show_demand_info_command(comboboxSelectedEvent):
    show_demand_info_thread = threading.Thread(target = show_demand_info, daemon = True)
    show_demand_info_thread.start()
    # app.after(0, check_show_demand_info, show_demand_info_thread)
    
def check_show_demand_info(show_demand_info_thread):
    if show_demand_info_thread.is_alive():
        app.after(100, check_show_demand_info, show_demand_info_thread)

def show_demand_info():
    app.read_data_btn['state'] = 'normal'
    app.run_simulation_btn['state'] = 'normal'
    app.run_optimization_btn['state'] = 'normal'
    app.statusbar.config(text = 'Retrieving last demand info...')
    # app.ax.cla()
    # app.ax = app.fig.add_subplot(111)
    # app.ax.xaxis_date()
    if app.connection_mode.get() == 'SQL Server':
        # app.manual_data_btn['state'] = 'normal'
        with app.connectToSQL() as connection:
            try:
                app.last_update_str.set('Retrieving data...')
                app.total_demand_str.set('Retrieving data...')
                app.rejected_pounds_str.set('Retrieving data...')
                #Bring time of last update
                # last_time, total_demand = connection.execute('SELECT * FROM "SAGE"."LOG"').first()
                last_time, total_demand, rejected_pounds = connection.execute('SELECT * FROM "SAGE"."LOG2"').first()
                app.last_update_str.set(last_time.strftime("%m/%d/%y %H:%M"))
                app.total_demand_str.set(f'{round(total_demand, 2):,}')
                app.rejected_pounds_str.set(f'{round(rejected_pounds,2):,}')
            except Exception as e:
                print('Could not connect to cloud database: ' + traceback.format_exc())
                # app.statusbar.config(text = 'Could not connect to cloud database.')
                app.last_update_str.set('Could not retrieve information.')
                app.total_demand_str.set('Could not retrieve information.')    
                app.rejected_pounds_str.set('Could not retrieve information.')
                app.statusbar.config(text = '')
                return
            try:
                # #Reading Error Demand table
                # ERROR_DEMAND = pd.read_sql_table('error_demand', schema = 'sac_output', con = connection)
                # app.rejected_pounds_str.set(f"{ERROR_DEMAND['Rejected Pounds'].sum():,.2f}")
                #Displaying demand graphic
                DEMAND = pd.read_sql_table('DEMAND', schema = 'ANYLOGIC', con = connection).astype({'Demand quantity (pounds)': float})
                ERROR_DEMAND = pd.read_sql_table('ERROR_DEMAND', schema = 'SAC_OUTPUT', con = connection)
            except Exception as e:
                print('Could not connect to cloud database: ' + traceback.format_exc())
                # app.statusbar.config(text = 'Could not connect to cloud database.')
                app.statusbar.config(text = '')
                return
    elif app.connection_mode.get() == 'Excel':
        try:
            with open('Model/Database Input/ld.log', 'r') as last_demand_info_log:
                last_time, total_demand, pounds_rejected = last_demand_info_log.readlines()
                app.last_update_str.set(last_time.strip())
                app.total_demand_str.set(total_demand.strip())
                app.rejected_pounds_str.set(pounds_rejected.strip())
        except Exception as e:
            print('Could not retrieve Demand data: ' + traceback.format_exc())
            app.statusbar.config(text = '')
            app.last_update_str.set('Could not retrieve information.')
            app.total_demand_str.set('Could not retrieve information.') 
            app.rejected_pounds_str.set('Could not retrieve information.')
        try:
            #Reading Error Demand table
            ERROR_DEMAND = pd.read_excel('Model/Database Input/Error_Demand.xlsx').astype(str)
        except:
            print('Could not read Error demand. ' + traceback.format_exc())
        try:
            #Displaying demand graphic
            DEMAND = pd.read_excel('Model/Demand.xlsx', sheet_name = 'Demand').astype({'Demand quantity (pounds)': float})
        except:
            print('Could not read Demand. ' + traceback.format_exc())
    app.statusbar.config(text = '')
    #Display graphic
    DEMAND['Due date'] = pd.to_datetime(DEMAND['Due date'])
    DEMAND['Week start'] = DEMAND['Due date'].map(lambda x: x - datetime.timedelta(x.weekday()))
    app.ax.clear()
    DEMAND = DEMAND[['Due date', 'Demand quantity (pounds)', 'Week start']].groupby('Week start', as_index = False).sum()
    app.plot = sns.barplot(x = "Week start", y = "Demand quantity (pounds)", data = DEMAND, 
                  estimator = sum, ci = None, ax = app.ax)
    x_dates = DEMAND['Week start'].dt.strftime('%m/%d/%Y')
    app.ax.set_xticklabels(labels = x_dates, rotation = 45, ha = 'right')
    app.fig.canvas.draw_idle()
    #Bring last error demand
    print('Displaying Error demand.')
    error_demand_pt = CustomTable(app.error_demand_frm, dataframe = ERROR_DEMAND, showtoolbar = False, showstatusbar = False, editable = False, enable_menus = False)
    error_demand_pt.adjustColumnWidths()
    error_demand_pt.show()
    right_notebook.tab(1, state = 'normal')
    # app.statusbar.config(text = '')

def update_db_from_SAGE_command():
    pgb_steps = 19
    if app.connection_mode.get() == 'SQL Server':
        pgb_steps += 15
    elif app.connection_mode.get() == 'Excel':
        pgb_steps += 1
    if app.to_excel.get():
        pgb_steps += 7
    lw = LoadingWindow(app, pgb_mode = 'indeterminate', pgb_maximum = pgb_steps)
    t = threading.Thread(target = input_generator.update_db_from_SAGE, daemon = True)
    t.start()
    app.root.after(0, lw.check, t)

def generate_model_files_from_backup_command():
    loading_window_backup = LoadingWindow(app.root, input_generator.generate_model_files_from_backup)

#Input generator (es necesario?)
input_generator = AlphiaInputGenerator(app)

app.add_data_lf(read_data_command = lambda: update_db_from_SAGE_command(), manual_data_command = lambda: add_manual_input(app))
app.read_data_btn['state'] = 'disabled'
app.manual_data_btn['state'] = 'disabled'

app.add_model_lf()

def run_experiment_cmd(experiment):
    app.statusbar.config(text = f'Starting {experiment} experiment...')
    # if app.connection_mode.get() == 'Excel':
        # steps = 7
    # elif app.connection_mode.get() == 'SAPHANACloud':
        # steps = 7
    # app.run_experiment_pgb = ttk.Progressbar(app.statusbar, mode = 'determinate', maximum = steps)
    # app.run_experiment_pgb.pack(side = 'left')
    th = threading.Thread(target = run_experiment, args = (experiment,), daemon = True)
    th.start()
    app.run_simulation_btn['state'] = 'disabled'
    app.run_optimization_btn['state'] = 'disabled'
    app.root.after(100, wait_for_experiment_process, th)
    
def run_experiment(experiment):
    try:
        i = 1
        if app.connection_mode.get() == 'SQL Server':
            limit = 10
        elif app.connection_mode.get() == 'Excel':
            limit = 9
        for line in run_experiment_batch(experiment):
            #limit so that it only shows the imports logs
            if i <= limit:
                app.statusbar.config(text = f'{line.strip()} ({i}/{limit})')
                i += 1
    except Exception as e:
        app.show_error(e, message = f"Error while running {experiment} experiment.")
        # app.running_simulation_var.set('')
        
def run_experiment_batch(experiment):
    p = subprocess.Popen(f'Model\DetSchedOpt_windows-{experiment}.bat ' + app.connection_mode.get().replace(" ", ""), shell = True, stdout = subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL, universal_newlines=True)
    for stdout_line in iter(p.stdout.readline, ""):
        yield stdout_line 
    p.stdout.close()
    return_code = p.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, f'Model\DetSchedOpt_windows-{experiment}.bat')
    
def wait_for_experiment_process(th):
    if th.is_alive():
        app.root.after(100, wait_for_experiment_process, th)
    else:
        app.run_simulation_btn['state'] = 'normal'
        app.run_optimization_btn['state'] = 'normal'
        app.statusbar.config(text = '')
        print('Experiment closed.')

app.run_simulation_btn['command'] = lambda: run_experiment_cmd('simulation')
app.run_simulation_btn['state'] = 'disabled'
app.run_optimization_btn['command'] = lambda: run_experiment_cmd('optimization')
app.run_optimization_btn['state'] = 'disabled'

buttons_dic = {'DEMAND REVIEW': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=223A9B02F4538FFC82411EFAF07F6A1D',
              'SCHEDULE REVIEW': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=C316E302FA989EB6B8DC0A7147C612B1',
              'RUN SUMMARY': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=4B636301B40D93B66DBA27FC1BF0C2C9',
              # 'SCHEDULE DETAIL': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=E86A9B02F45046DC9A422670A0016DA9',
              'SCHEDULE DETAIL': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#/story&/s/17E8E081E2F6C58A4660A2997AB5513D/?mode=view',
              'MASTER DATA ERRORS': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=315A9B02F45146C8478A9C88FAA53442',
              }
app.add_sac_buttons(buttons_dic, disabled = True)

right_frame = ttk.Frame(app.root)
right_frame.pack(side = tk.LEFT, fill = tk.BOTH, expand = True)

right_notebook = ttk.Notebook(right_frame)
right_notebook.pack(expand = True, fill = tk.BOTH)

display_info_frm = ttk.Frame(right_notebook)
right_notebook.add(display_info_frm, text = '  Main Tab   ')

main_upper_frm = ttk.Frame(display_info_frm)
main_upper_frm.pack(fill = tk.X)

main_upper_frm.columnconfigure(0, weight = 1, uniform = 'main_upper')
main_upper_frm.columnconfigure(1, weight = 1, uniform = 'main_upper')

import_settings_lf = ttk.LabelFrame(main_upper_frm, text = '    IMPORT/EXPORT SETTINGS')
import_settings_lf.grid(row = 0, column = 0, sticky = 'nwes', padx = 20, pady = 20)

connection_mode_label = ttk.Label(import_settings_lf, text = 'Select data source: ')
connection_mode_label.pack(pady = 10, padx = (40, 10), side = tk.LEFT)

app.connection_mode = tk.StringVar()
if debug:
    connection_combobox = ttk.Combobox(import_settings_lf, values = ['SQL Server', 'Excel'], textvariable = app.connection_mode, state = 'readonly')
else:
    connection_combobox = ttk.Combobox(import_settings_lf, values = ['SQL Server'], textvariable = app.connection_mode, state = 'readonly')
# connection_combobox.grid(column = 1, row = 0, padx = 40, pady = 10, sticky = 'w')
connection_combobox.pack(padx = 10, pady = 10, side = tk.LEFT)

#Bind event 'connection_combobox selected' to show_demand_info_command function
connection_combobox.bind('<<ComboboxSelected>>', show_demand_info_command)

app.to_excel = tk.IntVar()
to_excel_cb = ttk.Checkbutton(import_settings_lf, variable = app.to_excel, text = 'Save REST tables as\n Excel Files')
if debug:
    to_excel_cb.pack(padx = 40, pady = (0,10), side = tk.LEFT)

last_demand_lf = ttk.LabelFrame(main_upper_frm, text = '    CURRENT DEMAND')
last_demand_lf.grid(row = 0, column = 1, sticky = 'nwes', padx = 20, pady = 20)

labels_left_frame = ttk.Frame(last_demand_lf)
labels_left_frame.pack(side = tk.LEFT, padx = (40,30))

ttk.Label(labels_left_frame, text = 'Data generation date and time: ').pack(anchor = 'e', pady = 10)
ttk.Label(labels_left_frame, text = 'Total demand (in pounds): ').pack(anchor = 'e', pady = (0,10))
ttk.Label(labels_left_frame, text = 'Rejected pounds: ').pack(anchor = 'e', pady = (0,10))

labels_right_frame = ttk.Frame(last_demand_lf)
labels_right_frame.pack(side = tk.LEFT)

app.last_update_str = tk.StringVar(value = 'Select connection.')
ttk.Label(labels_right_frame, textvariable = app.last_update_str).pack(anchor = 'w', pady = 10)

app.total_demand_str = tk.StringVar(value = 'Select connection.')
ttk.Label(labels_right_frame, textvariable = app.total_demand_str).pack(anchor = 'w', pady = (0,10))

app.rejected_pounds_str = tk.StringVar(value = 'Select connection.')
ttk.Label(labels_right_frame, textvariable = app.rejected_pounds_str).pack(anchor = 'w', pady = (0,10))

app.grafic_lf = ttk.LabelFrame(display_info_frm, text = '   DEMAND PER WEEK')
app.grafic_lf.pack(anchor = 'n', padx = 20, pady = 20, fill = tk.X, expand = True)

app.error_demand_frm = ttk.Frame(right_notebook)
right_notebook.add(app.error_demand_frm, text = '  Error Demand   ', padding = 15)
right_notebook.tab(1, state = 'disabled')

sns.set_style('whitegrid', {'figure.facecolor': 'whitesmoke'})

app.fig = Figure(figsize = (10,4), tight_layout = True)
app.canvas = FigureCanvasTkAgg(app.fig, master = app.grafic_lf)

app.ax = app.fig.add_subplot(111)
app.ax.xaxis_date()

app.add_logo()

# app.canvas.draw()
app.canvas.get_tk_widget().pack(padx = 10, pady = 10, fill = tk.X)

app.root.mainloop()