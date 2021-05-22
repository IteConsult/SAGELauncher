from pandastable import *
# import tkinter as tk
from tkinter import *
import math
import pandas as pd

def getTextLength(text, w, font=None):
	"""Get correct canvas text size (chars) that will fit in \
	a given canvas width"""

	SCRATCH = Canvas()
	scratch = SCRATCH
	length = len(text)
	t = scratch.create_text((0,0), text=text, font=font)
	b = scratch.bbox(t)
	scratch.delete(t)
	twidth = b[2]-b[0]
	ratio = length/twidth
	length = math.floor(w*ratio)
	return twidth,length

class CustomTable(Table):

	def __init__(self, *args, **kwargs):
		Table.__init__(self, *args, **kwargs)
		
	def show(self, callback=None):
		"""Adds column header and scrollbars and combines them with
		   the current table adding all to the master frame provided in constructor.
		   Table is then redrawn."""

		#Add the table and header to the frame
		#ITE: We change RowHeader for CustomRowHeader
		self.rowheader = CustomRowHeader(self.parentframe, self)
		self.tablecolheader = ColumnHeader(self.parentframe, self, bg=self.colheadercolor)
		self.rowindexheader = IndexHeader(self.parentframe, self)
		self.Yscrollbar = AutoScrollbar(self.parentframe,orient=VERTICAL,command=self.set_yviews)
		self.Yscrollbar.grid(row=1,column=2,rowspan=1,sticky='news',pady=0,ipady=0)
		self.Xscrollbar = AutoScrollbar(self.parentframe,orient=HORIZONTAL,command=self.set_xviews)
		self.Xscrollbar.grid(row=2,column=1,columnspan=1,sticky='news')
		self['xscrollcommand'] = self.Xscrollbar.set
		self['yscrollcommand'] = self.Yscrollbar.set
		self.tablecolheader['xscrollcommand'] = self.Xscrollbar.set
		self.rowheader['yscrollcommand'] = self.Yscrollbar.set
		self.parentframe.rowconfigure(1,weight=1)
		self.parentframe.columnconfigure(1,weight=1)

		self.rowindexheader.grid(row=0,column=0,rowspan=1,sticky='news')
		self.tablecolheader.grid(row=0,column=1,rowspan=1,sticky='news')
		self.rowheader.grid(row=1,column=0,rowspan=1,sticky='news')
		self.grid(row=1,column=1,rowspan=1,sticky='news',pady=0,ipady=0)

		self.adjustColumnWidths()
		#bind redraw to resize, may trigger redraws when widgets added
		self.parentframe.bind("<Configure>", self.resized) #self.redrawVisible)
		self.tablecolheader.xview("moveto", 0)
		self.xview("moveto", 0)
		if self.showtoolbar == True:
			self.toolbar = ToolBar(self.parentframe, self)
			self.toolbar.grid(row=0,column=3,rowspan=2,sticky='news')
		if self.showstatusbar == True:
			self.statusbar = statusBar(self.parentframe, self)
			self.statusbar.grid(row=3,column=0,columnspan=2,sticky='ew')
		#self.redraw(callback=callback)
		self.currwidth = self.parentframe.winfo_width()
		self.currheight = self.parentframe.winfo_height()
		if hasattr(self, 'pf'):
			self.pf.updateData()
		return
			
	def adjustColumnWidths(self, limit=30):
		"""Optimally adjust col widths to accomodate the longest entry \
			in each column - usually only called on first redraw.
		Args:
			limit: max number of columns to resize
		"""

		fontsize = self.fontsize
		scale = self.getScale()
		if self.cols > limit:
			return
		self.cols = self.model.getColumnCount()
		for col in range(self.cols):
			colname = self.model.getColumnName(col)
			if colname in self.columnwidths:
				w = self.columnwidths[colname]
				#don't adjust large columns as user has probably resized them
				if w>200:
					continue
			else:
				w = self.cellwidth
			l = max(self.model.getlongestEntry(col), len(colname))
			txt = ''.join(['X' for i in range(l+1)])
			tw,tl = getTextLength(txt, self.maxcellwidth,
									   font=self.thefont)
			#print (col,txt,l,tw)
			if tw >= self.maxcellwidth:
				tw = self.maxcellwidth
			elif tw < self.cellwidth:
				tw = self.cellwidth
			self.columnwidths[colname] = tw
		return
		
class CustomRowHeader(RowHeader):
	def __init__(self, *args, **kwargs):
		RowHeader.__init__(self, *args, **kwargs)
	
	def handle_right_click(self, event):
		"""respond to a right click"""

		self.delete('tooltip')
		if hasattr(self, 'rightmenu'):
			self.rightmenu.destroy()
		#ITE: we don't want to allow menus
		#self.rightmenu = self.popupMenu(event, outside=1)
		return