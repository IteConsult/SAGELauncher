import tkinter as tk

new_window = None

def btn_function():
    global new_window
    new_window = tk.Toplevel()
    new_window.withdraw()
    
def btn_function2():
    new_window.deiconify()

root = tk.Tk()
btn = tk.Button(root, text = 'Withdraw', command = btn_function)
btn2 = tk.Button(root, text = 'Show', command = btn_function2)
btn.pack()
btn2.pack()

root.mainloop()