import tkinter as tk
from tkinter import ttk
import threading
import time

# def command1():
    # time.sleep(5)
    # print('done')

# def command2():
    # t1 = threading.Thread(target = command1, daemon = True)
    # t1.start()
    # print('hola')
    # root.after(10, command3, t1)
    # print('really really done')
    
# def command3(t1):
    # if t1.is_alive():
        # root.after(10, command3, t1)
    # else:
        # print('really done')
    
def f():
    print('hola')
    root.after(5, f)

root = tk.Tk()

# btn1 = ttk.Button(text = 'Open thread', command = command2)
# btn1.pack()

pgb = ttk.Progressbar()
pgb.pack()
pgb.start()

f()

root.mainloop()