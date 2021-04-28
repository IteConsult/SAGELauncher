import tkinter as tk
from tkinter import ttk
import pandas as pd
from CustomTable import CustomTable
import threading

class LoadingWindow(tk.Toplevel):
    def __init__(self, parent, command = None):
        self.size = (300, 80)
        tk.Toplevel.__init__(self, parent)
        s_width = parent.winfo_screenwidth()
        s_height = parent.winfo_screenheight()
        self.screen_position = lambda width, height: (width, height, (s_width - width)//2, (s_height - height)//2)
        self.geometry('%dx%d+%d+%d' % self.screen_position(*self.size))
        self.wm_protocol("WM_DELETE_WINDOW", lambda: self.close_window())
        self.title('Please wait...')
        self.loading_frame = ttk.Frame(self, style = 'LoadingWindow.TFrame')
        self.loading_frame.pack(expand = True, fill = tk.BOTH)
        self.loading_label = ttk.Label(self.loading_frame, text = 'Loading...')
        # self.loading_label.pack(anchor = 'w', padx = 10, pady = 10)
        self.loading_pgb = ttk.Progressbar(self.loading_frame, maximum = 20, mode = 'indeterminate')
        self.loading_pgb.pack(padx = 10, pady = (10,0), fill = tk.X)
        self.loading_pgb.start()
        self.cancel_btn = ttk.Button(self.loading_frame, text = 'Cancel', command = lambda: self.close_window())
        self.cancel_btn.pack(side = tk.BOTTOM, padx = 10, pady = (10,10), anchor = 'e')
        self.grab_set()
        self.focus_force()
        self.resizable(0, 0)
        self.transient(parent)
        self.thread = threading.Thread(target = command, args = (self,), daemon = True)
        self.thread.start()
        # self.thread.join()
        # self.destroy()

    def close_window(self):
        global connection_to_HANA
        print('Process interrupted.')
        self.destroy()
        #TODO find a better way to kill ongoing thread
        connection_to_HANA.close()
        connection_to_HANA = None

class ManualInput(tk.Toplevel):
    def __init__(self, root, connection, **iid_labels):     #table_name = table_label, por ejemplo: product_priority = 'Product priority'
        tk.Toplevel.__init__(self, root)
        self.title("Add Manual Input")
        self.connection = connection
        # self.state('zoomed')
        self.size = (900, 600)
        s_width = root.winfo_screenwidth()
        s_height = root.winfo_screenheight()
        screen_position = lambda width, height: (width, height, (s_width - width)//2, (s_height - height)//2)
        self.geometry('%dx%d+%d+%d' % screen_position(*self.size))
        self.transient(root)

        self.main_frame = ttk.Frame(self)
        # manual_window.resizable(0,0)
        self.main_frame.pack(expand = True, fill = tk.BOTH)

        self.treeview_frame = ttk.Frame(self.main_frame)
        self.treeview_frame.pack(side = tk.LEFT, fill = tk.Y, padx = 20, pady = 20)

        self.tables_treeview = ttk.Treeview(self.treeview_frame, selectmode = 'browse', height = 4)
        self.tables_treeview.heading("#0", text = "Select table", anchor = tk.W)
        for table_name in iid_labels:
            self.tables_treeview.insert('', 0, iid = table_name.lower(), text = iid_labels[table_name])
        self.tables_treeview.bind('<<TreeviewSelect>>', self.show_selected_table)
        self.tables_treeview.pack()

        # self.upload_from_file_btn = ttk.Button(self.treeview_frame, text = 'Select file', command = self.select_file) #TODO command
        # self.upload_from_file_btn.pack(pady = 20, anchor = 'e')

        self.tables_separator = ttk.Separator(self.main_frame, orient = 'vertical')
        self.tables_separator.pack(side = tk.LEFT, fill = tk.Y, padx = (0,20))

        self.tables_frame = ttk.Frame(self.main_frame)
        self.tables_frame.pack(side = tk.LEFT, fill = tk.BOTH, expand = True)
        self.tables_frame.grid_rowconfigure(1, weight = 1)
        self.tables_frame.grid_columnconfigure(0, weight = 1)

        self.frames_dic = {}
        self.modified_tables = {}

        for iid in iid_labels:
            setattr(self, iid+'_frm', TableFrame(self.tables_frame, iid, 'manual_files', con = connection))
            self.frames_dic[iid] = getattr(self, iid+'_frm')

        self.save_button = ttk.Button(self.tables_frame, text = 'Save', command = lambda: self.save_modifications())
        self.save_button.grid(row = 1, sticky = 'sw', padx = 20, pady = 20, ipadx = 10)
        
        self.upload_button = ttk.Button(self.tables_frame, text = 'Replace table with local file', command = lambda: self.select_file())
        self.upload_button.grid(row = 1, sticky = 'se', padx = 20, pady = 20, ipadx = 10)

    def show_selected_table(self, event):
        selected_iid = event.widget.focus()
        self.frames_dic[selected_iid].tkraise()

    def select_file(self):
        # filename =  tk.filedialog.askopenfilename(initialdir = "/", title = "Select file", filetypes = (("jpeg files","*.jpg"),("all files","*.*")) )
        filename =  tk.filedialog.askopenfilename(initialdir = "/", title = "Select file", filetypes = (("Excel sheet","*.xlsx"),) )
        if filename == '':
            return
        selected_iid = self.tables_treeview.focus()
        selected_pandastable = self.frames_dic[selected_iid].pt
        selected_pandastable.model.df = pd.read_excel(filename).astype(str)
        selected_pandastable.adjustColumnWidths()
        selected_pandastable.redraw()
        threading.Thread(target = self.upload_table, args = (selected_iid,)).start() 

    def upload_table(self, iid):
        table_frame = self.frames_dic[iid]
        df = table_frame.pt.model.df
        try:
            self.connection.execute(f'DELETE FROM "{table_frame.table_schema.upper()}"."{iid.upper()}"')
            df.to_sql(iid, schema = table_frame.table_schema, if_exists = 'append', con = self.connection, index = False)
            print('Update succesful.')
        except Exception as e:
            print('Couldn\'t update table in HANA: ' + str(e))
       
    def save_modifications(self):
        pass
        
class TableFrame(ttk.Frame):

    def __init__(self, parent, table_name, table_schema, con):
        tk.Frame.__init__(self, parent)
        self.con = con
        self.grid(row = 0, column = 0, sticky = 'nsew')
        self.table_name = table_name
        self.table_schema = table_schema
        self.table = pd.read_sql_table(table_name.lower(), schema = table_schema, con = con)
        self.pt = CustomTable(self, dataframe = self.table, showtoolbar = False, showstatusbar = False, editable = True)
        self.pt.adjustColumnWidths()
        self.pt.show()
        
    def replace_file(self):
        pass

if __name__ == '__main__':
    import sqlalchemy
    
    def create_window():
        connection_to_HANA = sqlalchemy.create_engine('hana://DBADMIN:HANAtest2908@8969f818-750f-468f-afff-3dc99a6e805b.hana.trial-us10.hanacloud.ondemand.com:443/?encrypt=true&validateCertificate=false').connect()
        mw = ManualInput(root, connection_to_HANA, extruders_schedule = "Extruders Schedule", families = "Families")

    root = tk.Tk()
    btn = tk.Button(text = 'Open window', command = create_window)
    btn.pack()
    
    root.mainloop()