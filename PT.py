from tkinter import *
from pandastable import Table, TableModel
import pandas as pd

class TestApp(Frame):
        """Basic test frame for the table"""
        def __init__(self, parent=None):
            self.parent = parent
            Frame.__init__(self)
            self.main = self.master
            self.main.geometry('600x400+200+100')
            self.main.title('Table app')
            f = Frame(self.main)
            f.pack(fill=BOTH,expand=1)
            df = pd.read_excel("C:/Users/admin/Desktop/ITE Consult/SAGE REST/WorkOrders.xlsx", index_col = 0)
            self.table = pt = Table(f, dataframe=df,
                                    showtoolbar=True, showstatusbar=True, editable = True)
            pt.show()
            return

app = TestApp()
#launch the app
app.mainloop()

# import tkinter as tk
# import pandastable
# import pandas as pd

# app = tk.Frame()
# app.geometry('600x400+200+100')
# app.title('Table app')
# f = tk.Frame(app)
# f.pack(fill = tk.BOTH, expand = 1)
# df = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]})
# pt = pandastable.Table(f, dataframe = df, showstatusbar = True, editable = True)
# pt.show()