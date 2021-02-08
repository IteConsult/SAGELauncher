import tkinter as tk
from tkinter import ttk
import threading
import time

def sleep_btn_command():
    sleep_thread = threading.Thread(target = time.sleep, args = (12,))
    print(sleep_thread)
    sleep_thread.start()
    sleep_progressbar.start()
    sleep_thread.join()
    sleep_progressbar.stop()

def nthreads():
    print(threading.enumerate())

window = tk.Tk()

sleep_btn = tk.Button(text = 'Sleep', command = lambda: threading.Thread(target = sleep_btn_command).start())
sleep_btn.pack()

sleep_progressbar = ttk.Progressbar(mode = 'indeterminate')
sleep_progressbar.pack()

nthreads_btn = tk.Button(text = 'Print threads', command = nthreads)
nthreads_btn.pack()

window.mainloop()