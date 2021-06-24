import tkinter as tk
from tkinter import ttk
import sqlalchemy
from sqlalchemy_hana import dialect
import time as time_module
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)
import pandas as pd
import datetime
import seaborn as sns
import sys
import os
sys.path.append(os.path.dirname(os.getcwd())+'\\LauncherClass')
import threading
from Launcher import Launcher, LoadingWindow
from CustomTable import CustomTable
from ManualInput import ManualInput
import traceback
from InputGeneration import * #TODO list functions
import subprocess

#This line prevents the bundled .exe from throwing a sqlalchemy-related error
sqlalchemy.dialects.registry.register('hana', 'sqlalchemy_hana.dialect', 'HANAHDBCLIDialect')

def connectToHANA(app):
    if not app.connection_to_HANA:
        try:
            app.connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
            print('Connection to cloud database established.')
            return 'Connection succesful.'
        except Exception as e:
            print('Connection failed!\n' + str(e))
            return 'Connection to cloud database failed! Retrying...'

def add_manual_input(app):
    lw = LoadingWindow(app)
    manual_window = ManualInput(app)
    load_tables_thread = threading.Thread(target = load_tables, args = (manual_window,), daemon = True)
    load_tables_thread.start()
    app.root.after(0, show_manual_window, app, manual_window, load_tables_thread, lw)    
    # lw.check(load_tables_thread, final_command = lambda: manual_window.show())
    
def load_tables(manual_window):
    manual_window.add_table('Extruders Schedule', 'extruders_schedule')
    manual_window.add_table('Families', 'families')
    manual_window.add_table('Product Priority', 'product_priority')
    manual_window.add_table('Customer Priority', 'customer_priority')
    
def show_manual_window(app, manual_window, load_tables_thread, lw):
    if load_tables_thread.is_alive():
        print('not yet')
        app.root.after(500, show_manual_window, app, manual_window, load_tables_thread, lw)
    else:
        print('now, show')
        lw.destroy()
        manual_window.show()

def startup():
    connected = False
    while not connected:
        out_string = connectToHANA(app)
        app.statusbar.config(text = out_string)
        if app.connection_to_HANA:
            connected = True
        else:
            time_module.sleep(5)

def show_start_info():
    app.statusbar.config(text = 'Connection established. Retrieving last demand info...')
    #Bring time of last update
    try:
        last_time, total_demand = app.connection_to_HANA.execute('SELECT * FROM "SAGE"."LOG"').first()
        app.last_update_str.set(last_time.strftime("%d/%m/%y %H:%M"))
        app.total_demand_str.set(str(round(total_demand, 2)))
    except:
        app.last_update_str.set('Couldn\'t retrieve information.')
        app.total_demand_str.set('Couldn\'t retrieve information.')
    #Displaying demand graphic
    df = pd.read_sql_table('demand', schema = 'anylogic', con = app.connection_to_HANA).astype({'Demand quantity (pounds)': float})
    df['Due date'] = pd.to_datetime(df['Due date'])
    df['Week start'] = df['Due date'].map(lambda x: x - datetime.timedelta(x.weekday()))
    df = df[['Due date', 'Demand quantity (pounds)', 'Week start']].groupby('Week start', as_index = False).sum()
    plot = sns.barplot(x = "Week start", y = "Demand quantity (pounds)", data = df, 
                  estimator = sum, ci = None, ax = app.ax)
    app.ax.xaxis_date()
    x_dates = df['Week start'].dt.strftime('%Y-%m-%d')
    app.ax.set_xticklabels(labels=x_dates, rotation=45, ha='right')
    app.canvas.get_tk_widget().pack(padx = 10, pady = 10, fill = tk.X)
    app.fig.canvas.draw_idle()
    #Bring last error demand
    ERROR_DEMAND = pd.read_sql_table('error_demand', schema = 'sac_output', con = app.connection_to_HANA)
    error_demand_pt = CustomTable(app.error_demand_frm, dataframe = ERROR_DEMAND, showtoolbar = False, showstatusbar = False, editable = False, enable_menus = False)
    error_demand_pt.adjustColumnWidths()
    error_demand_pt.show()
    right_notebook.tab(1, state = 'normal')
    app.statusbar.config(text = 'Connection established.')
    #Activate buttons
    app.read_data_btn['state'] = 'normal'
    # generate_model_files_from_backup_btn['state'] = 'normal'
    app.manual_data_btn['state'] = 'normal'

def wait_startup():
    if startup_thread.is_alive():
        app.root.after(2000, wait_startup)
    else:
        threading.Thread(target = lambda: show_start_info(), daemon = True).start()

def update_db_from_SAGE_command():
    lw = LoadingWindow(app)
    t = threading.Thread(target = input_generator.update_db_from_SAGE, daemon = True)
    t.start()
    app.root.after(0, lw.check, t)

def generate_model_files_from_backup_command():
    loading_window_backup = LoadingWindow(app.root, input_generator.generate_model_files_from_backup)

app = Launcher('DETAILED SCHEDULING OPTIMIZATION')
app.root.state('zoomed')
app.root.minsize(1520, 700)

#Debug variable
app.to_excel = False
#Connection
app.connection_to_HANA = None
input_generator = AlphiaInputGenerator(app)

app.add_data_lf(read_data_command = lambda: update_db_from_SAGE_command(), manual_data_command = lambda: add_manual_input(app))
app.read_data_btn['state'] = 'disabled'
app.manual_data_btn['state'] = 'disabled'

app.add_model_lf()

def run_simulation_cmd():
    lw = LoadingWindow(app)
    th = threading.Thread(target = subprocess.run, args = (f'Model\AlphiaVisual_windows-simulation.bat',))
    th.start()
    lw.check(th)
    
def run_optimization_cmd():
    lw = LoadingWindow(app)
    th = threading.Thread(target = subprocess.run, args = (f'Model\AlphiaVisual_windows-optimization.bat',))
    th.start()
    lw.check(th)

app.run_simulation_btn['command'] = lambda: run_simulation_cmd()
app.run_optimization_btn['command'] = lambda: run_optimization_cmd()

# buttons_dic = {'DEMAND REVIEW': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=223A9B02F4538FFC82411EFAF07F6A1D',
              # 'MASTER DATA ERRORS': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=315A9B02F45146C8478A9C88FAA53442',
              # 'RUN SUMMARY': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=4B636301B40D93B66DBA27FC1BF0C2C9',
              # 'SCHEDULE REVIEW (!)': 'http://www.google.com/',
              # 'REPORT CATALOG': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=home;tab=catalog',
              # 'SCHEDULE DETAIL': 'https://ite-consult.br10.hanacloudservices.cloud.sap/sap/fpa/ui/app.html#;view_id=story;storyId=E86A9B02F45046DC9A422670A0016DA9',
              # }
# app.add_sac_buttons(buttons_dic)

right_frame = ttk.Frame(app.root)
right_frame.pack(side = tk.LEFT, fill = tk.BOTH, expand = True)

right_notebook = ttk.Notebook(right_frame)
right_notebook.pack(expand = True, fill = tk.BOTH)

display_info_frm = ttk.Frame(right_notebook)
right_notebook.add(display_info_frm, text = '  Demand Info   ')

last_demand_lf = ttk.LabelFrame(display_info_frm, text = '    LAST DEMAND INFO')
last_demand_lf.pack(padx = 20, pady = 20, fill = tk.X)

labels_left_frame = ttk.Frame(last_demand_lf)
labels_left_frame.pack(side = tk.LEFT, padx = (40,30))

ttk.Label(labels_left_frame, text = 'Last update: ').pack(anchor = 'w', pady = 10)
ttk.Label(labels_left_frame, text = 'Total demand: ').pack(anchor = 'w', pady = (0,10))

labels_right_frame = ttk.Frame(last_demand_lf)
labels_right_frame.pack(side = tk.LEFT)

app.last_update_str = tk.StringVar(value = 'Retrieving...')
ttk.Label(labels_right_frame, textvariable = app.last_update_str).pack(anchor = 'w', pady = 10)

app.total_demand_str = tk.StringVar(value = 'Retrieving...')
ttk.Label(labels_right_frame, textvariable = app.total_demand_str).pack(anchor = 'w', pady = (0,10))

app.grafic_lf = ttk.LabelFrame(display_info_frm, text = '   DEMAND PER WEEK')
app.grafic_lf.pack(anchor = 'n', padx = 20, pady = 20, fill = tk.X, expand = True)

# app.display_info_widget = tk.Text(display_info_frm, wrap = 'word', state = 'disabled', relief = tk.FLAT, bg = 'white smoke')
# app.display_info_widget.pack(fill = tk.BOTH, expand = True, side = tk.LEFT, padx = (20, 0), pady = 20)
# display_info_ys = ttk.Scrollbar(display_info_frm, orient = 'vertical', command = app.display_info_widget.yview)
# display_info_ys.pack(side = tk.LEFT, fill = tk.Y)
# app.display_info_widget['yscrollcommand'] = display_info_ys.set

app.error_demand_frm = ttk.Frame(right_notebook)
right_notebook.add(app.error_demand_frm, text = '  Error Demand   ', padding = 15)
right_notebook.tab(1, state = 'disabled')

sns.set_style('whitegrid', {'figure.facecolor': 'whitesmoke'})

#print(sns.axes_style())
app.fig = Figure(figsize = (10,4), tight_layout = True)
app.ax = app.fig.add_subplot(111)
app.canvas = FigureCanvasTkAgg(app.fig, master = app.grafic_lf)
app.canvas.draw()

app.statusbar['text'] = 'Establishing connection to cloud database...'

app.add_logo()

startup_thread = threading.Thread(target = startup, daemon = True)
startup_thread.start()

app.root.after(1000, wait_startup)

app.root.mainloop()

if app.connection_to_HANA:
    app.connection_to_HANA.close()