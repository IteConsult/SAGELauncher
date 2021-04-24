import pandastable
import tkinter as tk
import math

def getTextLength(text, w, font=None):
	"""Get correct canvas text size (chars) that will fit in \
	a given canvas width"""

	SCRATCH = tk.Canvas()
	scratch = SCRATCH
	length = len(text)
	t = scratch.create_text((0,0), text=text, font=font)
	b = scratch.bbox(t)
	scratch.delete(t)
	twidth = b[2]-b[0]
	ratio = length/twidth
	length = math.floor(w*ratio)
	return twidth,length

class CustomTable(pandastable.Table):

	def __init__(self, *args, non_editable_columns = [], **kwargs):
		pandastable.Table.__init__(self, *args, **kwargs)
		self.non_editable_columns = non_editable_columns


	def drawCellEntry(self, row, col, text=None):
		"""When the user single/double clicks on a text/number cell,
		  bring up entry window and allow edits."""

		if self.editable == False:
			return
		#These two lines are the difference between this custom class and the general class:
		if col in self.non_editable_columns:
			return
		h = self.rowheight
		model = self.model
		text = self.model.getValueAt(row, col)
		if pd.isnull(text):
			text = ''
		x1,y1,x2,y2 = self.getCellCoords(row,col)
		w=x2-x1
		self.cellentryvar = txtvar = tk.StringVar()
		txtvar.set(text)

		self.cellentry = ttk.Entry(self.parentframe,width=20,
						textvariable=txtvar,
						takefocus=1,
						font=self.thefont)
		self.cellentry.icursor(tk.END)
		self.cellentry.bind('<Return>', lambda x: self.handleCellEntry(row,col))
		self.cellentry.focus_set()
		self.entrywin = self.create_window(x1,y1,
								width=w,height=h,
								window=self.cellentry,anchor='nw',
								tag='entry')
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
