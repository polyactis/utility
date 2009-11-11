#!/usr/bin/env python
"""
2009-11-11
	a gtk widget to display node status time-series data (MemUsed, NetLoad, LoadAve). Run it standalone will display some fake data.
"""
import pygtk
pygtk.require('2.0')
import gtk
from gtk import gdk

import matplotlib
matplotlib.use('GTKAgg')  # or 'GTK'
from matplotlib.backends.backend_gtkagg import FigureCanvasGTKAgg as FigureCanvas

from matplotlib.backends.backend_gtkagg import NavigationToolbar2GTKAgg as NavigationToolbar

from matplotlib.figure import Figure
from matplotlib.dates import DayLocator, HourLocator, DateFormatter, drange

class NodeStatusWindow(gtk.Window):
	def __init__(self, node_id=None, data=None):
		"""
		2009-11-11
			node_id is a string name for the node.
			data is an object that has 4 members, datetimeLs, MemUsedLs, NetLoadLs and LoadAveLs.
		"""
		gtk.Window.__init__(self)
		self.set_default_size(600, 600)
		self.connect('destroy', lambda win: self.destroy)	#gtk.main_quit())
		
		self.set_title('%s Status Plot'%node_id)
		self.set_border_width(8)
		
		vbox = gtk.VBox(False, 8)
		self.add(vbox)
		
		combobox = gtk.combo_box_new_text()
		slist = [ "MemUsed", "NetLoad", "LoadAve" ]
		for item in slist:
			combobox.append_text(item)
		combobox.connect('changed', self.plotData, data)
		
		# matplotlib stuff
		self.fig = Figure(figsize=(8,8))
		self.canvas = FigureCanvas(self.fig)  # a gtk.DrawingArea
		#self._idClick = self.canvas.mpl_connect('button_press_event', self.on_click)
		self.ax = self.fig.add_subplot(111)
		self.line, = self.ax.plot_date(data.datetimeLs, data.MemUsedLs)
		self.ax.set_title('MemUsed')
		self.ax.xaxis.set_major_formatter( DateFormatter('%Y-%m-%d %H:%M:%S') )

		self.ax.fmt_xdata = DateFormatter('%Y-%m-%d %H:%M:%S')
		self.fig.autofmt_xdate()	#automatically rotates the label if it's too long
		#self.line, = ax.plot(self.data[0,:], 'go')  # plot the first row
		
		
		combobox.set_active(0)	# after self.ax is defined. Otherwise the plotData() is called before self.ax exists. 
		
		"""
		combo = gtk.Combo()
		combo.set_popdown_strings(slist)
		combo.set_use_arrows(True)
		combo.set_use_arrows_always(True)
		combo.set_case_sensitive(False)
		combo.entry.connect("activate", self.plotData, data)
		"""
		vbox.pack_start(combobox, False, False)
		vbox.pack_start(self.canvas, True, True)
		self.canvas.draw()
		toolbar = NavigationToolbar(self.canvas, self)
		vbox.pack_start(toolbar, False, False)
		
	def plotData(self, combobox, data=None):
		"""
		2009-11-11
			call back when the combobox is changed
		"""
		model = combobox.get_model()
		index = combobox.get_active()
		dataType = model[index][0]
		
		# dataType = widget.get_text()
		
		#self.line.set_ydata(getattr(data, '%sLs'%dataType))
		self.ax.clear()
		self.ax.set_title(dataType)
		self.ax.plot_date(data.datetimeLs, getattr(data, '%sLs'%dataType))
		self.ax.xaxis.set_major_formatter( DateFormatter('%Y-%m-%d %H:%M:%S') )
		self.ax.fmt_xdata = DateFormatter('%Y-%m-%d %H:%M:%S')
		self.fig.autofmt_xdate()	#automatically rotates the label if it's too long
		
		self.canvas.draw()

if __name__ == '__main__':
	import sys, os, math
	sys.path.insert(0, os.path.expanduser('~/lib/python'))
	sys.path.insert(0, os.path.join(os.path.expanduser('~/script')))
	from pymodule import PassingData
	# generate some random data
	import datetime
	from numpy import arange
	date1 = datetime.datetime( 2000, 3, 2)
	date2 = datetime.datetime( 2000, 3, 6)
	delta = datetime.timedelta(hours=6)
	dates = drange(date1, date2, delta)
	y = arange(len(dates)*1.0)
	data = PassingData(datetimeLs=dates, MemUsedLs=y, NetLoadLs=y*y, LoadAveLs=y*y*y)
	
	manager = NodeStatusWindow(node_id='hpc1227', data=data)
	manager.connect('destroy', lambda win: gtk.main_quit())	#connect it with the main_quit(), rather than just destroy
	manager.show_all()
	gtk.main()
