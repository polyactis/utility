#!/usr/bin/env python
"""
2008-11-07
	upgraded to deal with a backend for hpc-cmb cluster. that backend stores data in a database.
	add more functions. check stdout/err/nodes of running jobs, etc
03-20-05
	gui program used to display jobs in the whole cluster
	given username and node_range
"""
import sys, os, math
#bit_number = math.log(sys.maxint)/math.log(2)
#if bit_number>40:       #64bit
sys.path.insert(0, os.path.expanduser('~/lib/python'))
sys.path.insert(0, os.path.join(os.path.expanduser('~/script')))
import os, sys, pygtk, time, re, math
pygtk.require('2.0')
import gnome	#2008-11-01 for new gnome related features
import gnome.ui
import gnomecanvas

import gtk, gtk.glade, gobject

from pymodule import yh_gnome
#simport foreach_cb, create_columns, fill_treeview
from cmb_cluster import cmb_cluster
from hpc_cmb_pbs import hpc_cmb_pbs
from datetime import datetime
from NodeStatusWindow import NodeStatusWindow

class grid_job_mgr(object):
	def __init__(self):
		"""
		2008-11-07
			upgraded to deal with a backend for hpc-cmb cluster. that backend stores data in a database.
			add more functions. check stdout/err/nodes of running jobs, etc
		2008-01-10
			use sys.argv[0] to figure out the path of grid_job_mgr.glade
		05-29-05
			add submit_option and no_of_nodes
		"""
		program_path = os.path.dirname(sys.argv[0])
		xml = gtk.glade.XML(os.path.join(program_path, 'grid_job_mgr.glade'))
		xml.signal_autoconnect(self)
		self.xml = xml
		self.app1 = xml.get_widget("app1")
		self.app1.connect("destroy", self.destroy)
		self.app1.set_size_request(1000,800)
		
		self.app1_appbar1 = xml.get_widget('app1_appbar1')
		if self.app1_appbar1:
			self.app1_appbar1.push('Status Message.')	#import gnome.ui has to be executed.
		
		
		self.treeview1 = xml.get_widget("treeview1")
		self.treeselection = self.treeview1.get_selection()
		self.treeview1.connect('row-activated', self.show_stdouterr)
		self.menu_job_right_click = gtk.Menu()	#xml.get_widget('menu_job_right_click')
		self.construct_menu_job_right_click(self.menu_job_right_click)
		#self.treeview1.connect('popup-menu', self.popup_menu_cb, self.menu_job_right_click)	#popup-menu is only for status icon
		self.treeview1.connect('button-release-event', self.rows_selected, self.app1_appbar1, self.menu_job_right_click)
		#self.treeview1.connect('cursor-changed', self.job_rows_selected, self.app1_appbar1)	#2008-11-06 'button-release-event' is better than 'cursor-changed'. the latter has a lag and display the #selected-rows one click ahead.
		
		self.treeview_nodes = xml.get_widget('treeview_nodes')
		self.tree_nodes_selection = self.treeview_nodes.get_selection()
		self.treeview_nodes.connect('row-activated', self.log_into_node)
		self.menu_node_right_click = gtk.Menu()
		self.construct_menu_node_right_click(self.menu_node_right_click)
		self.treeview_nodes.connect('button-release-event', self.rows_selected, self.app1_appbar1, \
								self.menu_node_right_click, 'node')
		
		self.combobox_backend_choice = xml.get_widget('combobox_backend_choice')
		self.entry_username = xml.get_widget("entry_username")
		self.entry_username.set_text("yuhuang")
		self.entry_node_range = xml.get_widget("entry_node_range")
		self.entry_node_range.set_text("16")
		self.entry_jobs_since = xml.get_widget("entry_jobs_since")
		self.checkbutton_running = xml.get_widget('checkbutton_running')
		self.checkbutton_update_node_info = xml.get_widget('checkbutton_update_node_info')
		self.checkbutton_update_job_info = xml.get_widget('checkbutton_update_job_info')
		self.entry_job_state_to_mark = xml.get_widget('entry_job_state_to_mark')
		self.ps_command = 'ps'
		self.tmp_fname = '/tmp/%sjobs'%(time.time())
		
		
		#the job submission dialog
		self.dialog_submit = xml.get_widget("dialog_submit")
		self.dialog_submit.hide()
		#how to handle the close window action?
		
		self.dialog_submit.connect("delete_event", self.dialog_submit_hide)
		#self.dialog_submit.connect("destroy", self.on_cancelbutton_submit_clicked)		
		self.textview_submit = xml.get_widget("textview_submit")
		self.radiobutton_qsub = xml.get_widget("radiobutton_qsub")	#05-29-05
		self.radiobutton_qsub.connect("toggled", self.on_radiobutton_submit_toggled, "radiobutton_qsub")
		self.radiobutton_at = xml.get_widget("radiobutton_at")	#05-29-05
		self.radiobutton_at.connect("toggled", self.on_radiobutton_submit_toggled, "radiobutton_at")
		self.radiobutton_at16 = xml.get_widget("radiobutton_at16")	#05-29-05
		self.radiobutton_at16.connect("toggled", self.on_radiobutton_submit_toggled, "radiobutton_at16")
		self.checkbutton_runtime_stdout = xml.get_widget("checkbutton_runtime_stdout")	#2008-11-03
		self.entry_qsub_option = xml.get_widget("entry_qsub_option")	#06-07-05
		self.entry_walltime = xml.get_widget("entry_walltime")	#2008-11-03
		self.entry_workdir = xml.get_widget("entry_workdir")	#2008-11-03
		self.entry_job_file_directory = xml.get_widget("entry_job_file_directory")	#2008-11-03
		self.entry_job_name_prefix = xml.get_widget("entry_job_name_prefix")	#05-22-05
		self.entry_job_starting_number = xml.get_widget("entry_job_starting_number")
		self.entry_time = xml.get_widget("entry_time")	#05-22-05
		
		self.entry_no_of_nodes = xml.get_widget("entry_no_of_nodes")	#05-29-05
		self.entry_ppn = xml.get_widget("entry_ppn")	#05-29-05
		self.entry_node_range_submit = xml.get_widget("entry_node_range_submit")
		
		self.job_starting_number = 0
		self.job_fprefix = 'grid_job_mgr'
		self.submit_option = 1	#05-29-05	default is 1(qsub), changed in on_radiobutton_qsub_toggled()
		self.submit_option_dict = {'radiobutton_qsub':1,
			'radiobutton_at':2,
			'radiobutton_at16':3}	#05-29-05	which option to take.
		
		
		#2008-11-1 setup to redirect stdout/stderr to textbuffer_output
		self.textview_output = xml.get_widget('textview_output')
		self.textbuffer_output = self.textview_output.get_buffer()
		#redirect stdout/stderr to textbuffer_output
		t_table=self.textbuffer_output.get_tag_table()
		tag_err=gtk.TextTag("error")
		tag_err.set_property("foreground","red")
		#tag_err.set_property("font","monospace 10")
		t_table.add(tag_err)
		tag_out=gtk.TextTag("output")
		tag_out.set_property("foreground","blue")
		#tag_out.set_property("font","monospace 10")
		t_table.add(tag_out)
		
		self.dummy_out = yh_gnome.Dummy_File(self.textbuffer_output, tag_out)
		self.dummy_err = yh_gnome.Dummy_File(self.textbuffer_output, tag_err)
		
		#the job stdout dialog
		self.dialog_job_stdout = xml.get_widget("dialog_job_stdout")
		self.dialog_job_stdout.hide()
		self.dialog_job_stdout.connect("delete_event", yh_gnome.subwindow_hide)
		self.textview_job_stdout = xml.get_widget('textview_job_stdout')
		self.textbuffer_job_stdout = self.textview_job_stdout.get_buffer()
		
		#add a red tag to textbuffer_job_stdout in order to output job id conspicuously
		job_id_tag=gtk.TextTag("job_id")
		job_id_tag.set_property("foreground","red")
		self.textbuffer_job_stdout.get_tag_table().add(job_id_tag)
		self.job_id_tag = job_id_tag
		
		
		self.dialog_confirm_kill = xml.get_widget('dialog_confirm_kill')
		self.dialog_confirm_kill.hide()
		self.dialog_confirm_kill.connect("delete_event", yh_gnome.subwindow_hide)
		self.textview_confirm_kill = xml.get_widget("textview_confirm_kill")
		
		self.backend_class_dict = {0:hpc_cmb_pbs,
			1:cmb_cluster}
		
		self.backend_ins = None
		
		self.no_of_refreshes = 0
		self.tvcolumn_dict = {}
		self.cell_dict = {}
		
		self.node_queue_app = xml.get_widget("node_queue_app")
		self.node_queue_app.connect("delete_event", yh_gnome.subwindow_hide)
		self.node_queue_app.set_size_request(1000,800)	
		self.node_queue_appbar = xml.get_widget('node_queue_appbar')
		
		self.treeview_node_queue = xml.get_widget("treeview_node_queue")
		self.treeview_node_queue.connect('row-activated', self.on_button_plot_node_status_clicked)
		self.menu_node_queue_right_click = gtk.Menu()
		self.construct_menu_node_queue_right_click(self.menu_node_queue_right_click)
		self.treeview_node_queue.connect('button-release-event', self.rows_selected, self.node_queue_appbar, \
										self.menu_node_queue_right_click, 'node')
		
		button_show_node_q_window = xml.get_widget('button_show_node_q_window')
		button_show_node_q_window.connect('clicked', self.on_menuitem_show_node_q_window_activate)
		
		self.app1.show_all()
		
		self.job_refresh_timeout_source_id = None
		self.node_queue_refresh_timeout_source_id = None
		
		#2008-11-05 connect stdout/stderr in the end to avoid masquerading the error output produced by bugs above
		#sys.stdout = self.dummy_out		#2008-11-06 this will deadlocks the program sometimes when lots of stdout and stderr are being directed to it
		#sys.stderr = self.dummy_err		
		
	
	def on_checkbutton_running_toggled(self, checkbutton_running):
		"""
		03-20-05
			control the ps command to get only running processes or all
		"""
		if checkbutton_running.get_active() == 0:
			self.ps_command = 'ps'
		else:
			self.ps_command = 'ps r'
		print "ps_command is %s"%self.ps_command
	
	timedelta_pattern = re.compile(r'(\d+)([smhd])*')	# used to parse time interval. s=seconds, m=minutes, h=hours, d=days. Default is seconds.
	
	time_unit2milliseconds = {'s': 1000, 'm':60000, 'h': 3600000, 'd':86400000}
	
	def getTimeIntervalFromStr(self, interval_str):
		"""
		2009-11-10
			parse time interval. like '20s', '20m', etc. s=seconds, m=minutes, h=hours, d=days.
		"""
		timedelta_p_search_result = self.timedelta_pattern.search(interval_str)
		if timedelta_p_search_result:
			interval_number, time_unit = timedelta_p_search_result.groups()[:2]
			interval_number = int(interval_number)
		else:
			interval_number = 20
			time_unit = 's'
		if not time_unit:
			time_unit = 's'
		interval = self.time_unit2milliseconds[time_unit]*interval_number
		return interval
	
	def on_button_refresh_clicked(self, button_refresh=None, data=None, update_job_info=None):
		"""
		2009-11-10
			either call refreshJobs()
			or setup a timeout function to call refreshJobs() periodically
		03-20-05
			fill in the treeview1
			--create_columns()	on first refresh
		"""
		entry_job_refresh_interval = self.xml.get_widget("entry_job_refresh_interval")
		togglebutton_refresh_on_demand = self.xml.get_widget("togglebutton_refresh_on_demand")	# 2009-11-10
		if togglebutton_refresh_on_demand.get_active():
			self.refreshJobs(button_refresh=button_refresh, data=data, update_job_info=update_job_info)
		else:
			self.refreshJobs(button_refresh=button_refresh, data=data, update_job_info=update_job_info)
			
			if self.job_refresh_timeout_source_id is not None:
				gobject.source_remove(self.job_refresh_timeout_source_id)
			interval = self.getTimeIntervalFromStr(entry_job_refresh_interval.get_text())
			self.job_refresh_timeout_source_id = gobject.timeout_add(interval, self.refreshJobs)
	
	def on_togglebutton_refresh_on_demand_toggled(self, widget, data=None):
		"""
		2009-11-10
			remove the timeout function 
		"""
		if self.job_refresh_timeout_source_id is not None:
			gobject.source_remove(self.job_refresh_timeout_source_id)
	
	def refreshJobs(self, button_refresh=None, data=None, update_job_info=None):
		"""
		2009-11-10
			split out of on_button_refresh_clicked()
		"""
		if self.backend_ins is None:
			sys.stderr.write("Backend is not selected yet! Select one.\n")
			return False	# 2009-11-10 to stop the timeout
		
		update_node_info = self.checkbutton_update_node_info.get_active()
		if update_job_info is None:	#if it's none, get it from the checkbutton
			update_job_info = self.checkbutton_update_job_info.get_active()
		username = self.entry_username.get_text()
		jobs_since = self.entry_jobs_since.get_text()
		jobs_since = datetime.strptime(jobs_since, '%Y-%m-%d %H:%M')
		only_running = self.checkbutton_running.get_active()
		"""
		real_node_range = self.parse_node_range(self.entry_node_range.get_text())
		node2ps = self.node_process_instance.get_processes(real_node_range, self.ps_command, username, self.tmp_fname)
		
		if node2ps=={}:
			sys.stderr.write("No process data fetched.\n")
			return
		
		if self.no_of_refreshes == 0:
			#extra work is needed in the first refresh
			self.create_columns(node2ps.values()[0].label_list)
		self.liststore = gtk.ListStore(str, str, str, str, str, str, str, str, str)	#nine columns
		
		for node in node2ps:
			for ps_output in node2ps[node].ps_output_list:
				if len(ps_output)==8:	#cmd is not null
					self.liststore.append([node]+ps_output)
		"""
		yh_gnome.create_columns(self.treeview1, self.backend_ins.display_job_label_ls)
		self.liststore = gtk.ListStore(*self.backend_ins.display_job_label_type_ls)
		list_2d = self.backend_ins.refresh(username, update_node_info, jobs_since, only_running, \
										update_job_info=update_job_info)
		yh_gnome.fill_treeview(self.treeview1, self.liststore, list_2d, reorderable=True, multi_selection=True)
		"""
		# set the TreeView mode to be liststore
		self.treeview1.set_model(self.liststore)

		
		for i in range(len(ps_output)+1):
			# make it searchable
			self.treeview1.set_search_column(i)
			
		# Allow drag and drop reordering of rows
		self.treeview1.set_reorderable(True)
		#setting the selection mode
		self.treeselection.set_mode(gtk.SELECTION_MULTIPLE)
		"""
		self.no_of_refreshes += 1
		return True	#2009-11-10 return True in order for the timeout to continue
		
	def create_columns(self, label_list):
		"""
		2008-11-03
			deprecated, use yh_gnome.create_columns instead
		03-20-05
			create columns in the treeview in the first refresh
		"""
		self.tvcolumn_dict[0] = gtk.TreeViewColumn('node')	# create the TreeViewColumn to display the data
		self.treeview1.append_column(self.tvcolumn_dict[0])	# add tvcolumn to treeview
		self.cell_dict[0] = gtk.CellRendererText()	# create a CellRendererText to render the data
		self.tvcolumn_dict[0].pack_start(self.cell_dict[0], True)	# add the cell to the tvcolumn and allow it to expand
		# set the cell "text" attribute to column 0 - retrieve text
		# from that column in liststore
		self.tvcolumn_dict[0].add_attribute(self.cell_dict[0], 'text', 0)
		# Allow sorting on the column
		self.tvcolumn_dict[0].set_sort_column_id(0)
		for i in range(len(label_list)):
			self.tvcolumn_dict[i+1] = gtk.TreeViewColumn(label_list[i])
			self.treeview1.append_column(self.tvcolumn_dict[i+1])
			self.cell_dict[i+1] = gtk.CellRendererText()
			self.tvcolumn_dict[i+1].pack_start(self.cell_dict[i+1], True)
			self.tvcolumn_dict[i+1].add_attribute(self.cell_dict[i+1], 'text', i+1)
			self.tvcolumn_dict[i+1].set_sort_column_id(i+1)

	def on_button_kill_clicked(self, button_kill):
		"""
		03-20-05
			kill selected processes
		"""
		#not_sure = raw_input("Are you sure? y/n")
		#if not_sure!='y':
		#	print "Aborted."
		#	return
		textbuffer = self.textview_confirm_kill.get_buffer()
		textbuffer.set_text('')
		iter=textbuffer.get_end_iter()
		textbuffer.insert(iter, "Really wanna kill selected jobs?")
		self.dialog_confirm_kill.show()
		
	def on_button_submit_clicked(self, button_submit):
		"""
		03-21-05
			open a dialog to submit jobs
		"""
		self.entry_job_starting_number.set_text(repr(self.job_starting_number))
		self.dialog_submit.show()
	
	def on_button_resubmit_clicked(self, button_resubmit):
		"""
		2008-11-03
		"""
		textbuffer_submit = self.textview_submit.get_buffer()
		iter=textbuffer_submit.get_end_iter()
		
		pathlist = []
		self.treeselection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				job_id = self.liststore[pathlist[i][0]][0]
				job = self.backend_ins.get_job(job_id)
				textbuffer_submit.insert(iter, job.content)
				self.dialog_submit.set_title('Job %s (%s)'%(job.id, job.short_name))
		self.dialog_submit.show()
	
	def on_okbutton_submit_clicked(self, okbutton_submit):
		"""
		2008-11-03
			adjust to the new
		03-21-05
			call node_process_instance to submit jobs
		05-22-05
		
		05-29-05
			add submit_option and no_of_nodes
		06-07-05
			add qsub_option
		"""
		runtime_output_stdout = self.checkbutton_runtime_stdout.get_active()
		qsub_option = self.entry_qsub_option.get_text()	#06-07-05
		walltime = self.entry_walltime.get_text()
		workdir = self.entry_workdir.get_text()
		no_of_nodes = self.entry_no_of_nodes.get_text()	#05-29-95
		no_of_nodes = int(no_of_nodes)
		ppn = self.entry_ppn.get_text()
		ppn = int(ppn)
		job_file_dir = self.entry_job_file_directory.get_text()
		job_name_prefix = self.entry_job_name_prefix.get_text()	#05-22-05
		
		self.job_starting_number = self.entry_job_starting_number.get_text()
		if self.job_starting_number:
			self.job_starting_number = int(self.job_starting_number)
		else:
			sys.stderr.write("What's the job_starting_number?\n")
			return
		"""
		time_to_run_jobs = self.entry_time.get_text()	#05-22-05
		node_range = self.entry_node_range_submit.get_text()
		if node_range:
			node_range = self.parse_node_range(node_range)
		else:
			sys.stderr.write("Which nodes to run these jobs?\n")
			return
		"""
		
		textbuffer = self.textview_submit.get_buffer()
		startiter, enditer = textbuffer.get_bounds()
		job_content  = textbuffer.get_text(startiter, enditer)
		#job_list = text.split("\n")	#2008-11-03 backend decides what to do
		if not job_name_prefix:
			job_name_prefix = self.job_fprefix
		
		self.job_starting_number = self.backend_ins.submit_job(job_content, job_file_dir, job_name_prefix, \
															self.job_starting_number, no_of_nodes, \
				qsub_option, ppn, submit_option=qsub_option, workdir=workdir, walltime=walltime, \
				runtime_output_stdout=runtime_output_stdout)
		
		self.entry_job_starting_number.set_text(repr(self.job_starting_number))
		#self.job_starting_number = self.node_process_instance.submit_jobs(job_list, \
		#	node_range, job_name_prefix, self.job_starting_number, time_to_run_jobs, \
		#	self.submit_option, no_of_nodes, qsub_option)
		self.dialog_submit.hide()
		
	def on_cancelbutton_submit_clicked(self, cancelbutton_submit):
		self.dialog_submit.hide()
		return True
		
	def dialog_submit_hide(self, widget, event, data=None):
		widget.hide()
		return True
	
	def parse_node_range(self, node_range):
		"""
		03-21-05
			input: node_range i.e. 1-3,5,8-10
			output: real_node_range, i.e. [1,2,3,5,8,9,10]
		10-23-05
			handle the situation to include nodes like app2, app1, ..
		"""
		number_p = re.compile(r'^\d')	#10-23-05
		node_functor = lambda x: 'node'+str(x)	#prepend 'node' before a number
		real_node_range = []
		node_range = node_range.split(',')
		for nodes in node_range:
			if number_p.match(nodes):	#10-23-05
				nodes = nodes.split('-')
				nodes = map(int, nodes)
				if len(nodes)==2:
					nodes = range(nodes[0], nodes[1]+1)
				nodes = map(node_functor, nodes)
				real_node_range += nodes
			else:
				real_node_range.append(nodes)
		return real_node_range
		
	def destroy(self, widget):
		if os.path.isfile(self.tmp_fname):
			try:
				os.remove(self.tmp_fname)
			except:
				sys.stderr.write("Error while removing %s.\n"%self.tmp_fname)
		gtk.main_quit()
	
	def on_radiobutton_submit_toggled(self, widget, event=None, data=None):
		"""
		05-29-05
		"""
		if widget.get_active() == 1:	#only change self.submit_option to the active radiobutton
			self.submit_option = self.submit_option_dict[data]
	
	def on_combobox_backend_choice_changed(self, widget, data=None):
		"""
		2011-4-28
			fetch db_user, db_passwd, db_host, db_port from the GUI and pass them to backend class
		2008-11-02
			change the underlying backend
		"""
		#self.node_process_instance = node_process()	#the backend class
		self.class_chosen = self.combobox_backend_choice.get_active()
		self.backend_doc = self.backend_class_dict[self.class_chosen].__doc__
		self.app1.set_title(self.combobox_backend_choice.get_active_text())
		#self.app_input.set_title(self.combobox_qc_class_choice.get_active_text())
		
		db_user = yh_gnome.getDataOutOfTextEntry(self.xml.get_widget("entry_db_username"), data_type=str,\
												default='crocea')
		db_passwd = yh_gnome.getDataOutOfTextEntry(self.xml.get_widget("entry_db_passwd"), data_type=str)
		db_host = yh_gnome.getDataOutOfTextEntry(self.xml.get_widget("entry_db_host"), data_type=str)
		db_port = yh_gnome.getDataOutOfTextEntry(self.xml.get_widget("entry_db_port"), data_type=int)
		self.backend_ins = self.backend_class_dict[self.class_chosen](db_user=db_user, cluster_username=self.entry_username.get_text(),\
										db_passwd=db_passwd, hostname=db_host, dbname='graphdb', schema='cluster_job', port=db_port)
	def show_stdouterr(self, treeview, path, view_column):
		self.on_button_check_stdouterr_clicked(treeview)
	
	
	def on_button_check_stdouterr_clicked(self, widget, data=None):
		"""
		2008-11-02
			fetch the job_stdout and/or job_stderr from the db
		"""
		pathlist = []
		self.treeselection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		textbuffer_job_stdout = self.textview_job_stdout.get_buffer()
		iter=textbuffer_job_stdout.get_end_iter()
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				job_id = self.liststore[pathlist[i][0]][0]
				job = self.backend_ins.get_job(job_id)
				stdouterr = job.job_stdout
				if job.job_stderr_fname:
					stdouterr += job.job_stderr
				#stdouterr = self.backend_ins.fetch_job_stdouterr(job_id)
				textbuffer_job_stdout.insert_with_tags(iter, '\t\t%s\n'%job_id, self.job_id_tag)
				if stdouterr:
					textbuffer_job_stdout.insert(iter, stdouterr)
				self.dialog_job_stdout.set_title("Job %s (%s)"%(job.id, job.short_name))
		self.dialog_job_stdout.show()
	
	def on_button_cancel_job_stdout_clicked(self, widget, event=None):
		self.dialog_job_stdout.hide()
	
	def on_button_clear_job_stdout_clicked(self, widget, data=None):
		textbuffer_job_stdout = self.textview_job_stdout.get_buffer()
		textbuffer_job_stdout.set_text('')
		self.dialog_job_stdout.set_title('')
	
	def on_button_confirm_kill_clicked(self, widget, data=None):
		"""
		2008-11-04
		"""
		pathlist = []
		self.treeselection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				job_id = self.liststore[pathlist[i][0]][0]
				#pid = self.liststore[pathlist[i][0]][1]
				self.backend_ins.kill_job(job_id)
				#self.node_process_instance.kill_process(node_number, pid)
		else:
			sys.stderr.write("Have you selected processes?\n")
		self.dialog_confirm_kill.hide()
	
	def on_button_confirm_cancel_clicked(self, widget, data=None):
		"""
		2008-11-04
		"""
		self.dialog_confirm_kill.hide()
	
	def log_into_node(self, widget, event=None, data=None):
		"""
		2008-11-04
			executing command following brings you into the node.
			
			gnome-terminal -e "ssh -t yuhuang@hpc-cmb ssh hpc0718"
		"""
		pathlist = []
		self.tree_nodes_selection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				node_id = self.liststore_nodes[pathlist[i][0]][0]
				self.backend_ins.log_into_node(node_id)
	
	def on_button_dialog_submit_clear_clicked(self, widget, data=None):
		textbuffer = self.textview_submit.get_buffer()
		textbuffer.set_text('')
		self.dialog_submit.set_title('Submit new jobs')
		
	
	def on_button_mark_wrong_clicked(self, widget, data=None):
		"""
		2008-11-05
			mark state of selected jobs as wrong,
		"""
		pathlist = []
		self.treeselection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		how_job_ended_id = self.entry_job_state_to_mark.get_text()
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				job_id = self.liststore[pathlist[i][0]][0]
				#pid = self.liststore[pathlist[i][0]][1]
				self.backend_ins.mark_one_job_wrong(job_id, how_job_ended_id)
		
		#update the job view in the main app by only fetching info from db without updating job info in db
		self.refreshJobs(update_job_info=False)
	
	
	def on_button_refresh_job_stdout_clicked(self, widget, data=None):
		"""
		2008-11-05
			respond to the refresh button in dialog_job_stdout
			
			update the information of jobs inside the db
			
			
			almost same as on_button_check_stdouterr_clicked(), except updateOneJobInDB() for every job
		"""
		pathlist = []
		self.treeselection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		textbuffer_job_stdout = self.textview_job_stdout.get_buffer()
		iter=textbuffer_job_stdout.get_end_iter()
		username = self.entry_username.get_text()
		
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				job_id = self.liststore[pathlist[i][0]][0]
				self.backend_ins.updateOneJobInDB(job_id, username=username)	
				
				job = self.backend_ins.get_job(job_id)
				stdouterr = job.job_stdout
				if job.job_stderr_fname:
					stdouterr += job.job_stderr
				#stdouterr = self.backend_ins.fetch_job_stdouterr(job_id)
				textbuffer_job_stdout.insert_with_tags(iter, '\t\t%s\n'%job_id, self.job_id_tag)
				if stdouterr:
					textbuffer_job_stdout.insert(iter, stdouterr)
				self.dialog_job_stdout.set_title("Job %s (%s)"%(job.id, job.short_name))
		
		#update the job view in the main app by only fetching info from db without updating job info in db
		#self.refreshJobs(update_job_info=False)
	
	def on_checkbutton_redirect_stdouterr_toggled(self, checkbutton):	
		if checkbutton.get_active() == 1:
			#2008-11-05 connect stdout/stderr in the end to avoid masquerading the error output produced by bugs above
			sys.stdout = self.dummy_out		#2008-11-06 this will deadlocks the program sometimes when lots of stdout and stderr are being directed to it
			sys.stderr = self.dummy_err
		else:
			sys.stdout = sys.__stdout__
			sys.stderr = sys.__stderr__
	
	def construct_menu_job_right_click(self, menu_job_right_click):
		"""
		2009-2-16
			add menuitem 'show when this job starts/ed'
		2008-11-07
			add a 3rd menu 'resubmit this job'
		2008-11-06
			construct a popup menu for the right click on a job entry
		"""
		
		menuItem = gtk.MenuItem('show nodes')
		menuItem.connect('activate', self.show_nodes_of_this_job)
		#sm = gtk.Menu()
		#menuItem.set_submenu(sm)
		menu_job_right_click.append(menuItem)
		menuItem2 = gtk.MenuItem('show stdout/err')
		menuItem2.connect('activate', self.on_button_check_stdouterr_clicked)
		menu_job_right_click.append(menuItem2)
		
		menuItem3 = gtk.MenuItem('resubmit this job')
		menuItem3.connect('activate', self.on_button_resubmit_clicked)
		menu_job_right_click.append(menuItem3)
		
		menuItem4 = gtk.MenuItem('show when this job starts/ed')
		menuItem4.connect('activate', self.showstartJob)
		menu_job_right_click.append(menuItem4)
		
		menuItem4 = gtk.MenuItem('kill selected jobs')
		menuItem4.connect('activate', self.on_button_kill_clicked)
		menu_job_right_click.append(menuItem4)
		
		#sm.append(menuItem2)
		#menu.append(menuItem)
		
		#statusIcon.set_from_stock(gtk.STOCK_HOME)
		#statusIcon.set_tooltip("StatusIcon test")
		#statusIcon.connect('popup-menu', popup_menu_cb, menu)
		#statusIcon.set_visible(True)
	
	def construct_menu_node_right_click(self, menu):
		"""
		2008-11-06
			construct a popup menu for the right click on a job entry
		"""
		
		menuItem = gtk.MenuItem('Log into selected node(s)')
		menuItem.connect('activate', self.log_into_node)
		menu.append(menuItem)
		
		menuItem = gtk.MenuItem('Add selected node(s) into checking queue')
		menuItem.connect('activate', self.on_button_add_node_clicked)
		menu.append(menuItem)
	
	def construct_menu_node_queue_right_click(self, menu):
		"""
		2009-11-10
			the right-click menu for the treeview_node_queue.
		"""
		menuItem = gtk.MenuItem('Remove Selected Node(s)')
		menuItem.connect('activate', self.on_button_remove_node_clicked)
		menu.append(menuItem)
		
		menuItem = gtk.MenuItem('Plot Selected Node(s)')
		menuItem.connect('activate', self.on_button_plot_node_status_clicked)
		menu.append(menuItem)
		
		menuItem = gtk.MenuItem('Log into selected node(s)')
		menuItem.connect('activate', self.log_into_node)
		menu.append(menuItem)
		
	def get_selected_job_id_ls(self):
		"""
		2008-11-07
			a very common function which should be refactored long ago
		"""
		pathlist = []
		self.treeselection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		job_id_ls = []
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				job_id = self.liststore[pathlist[i][0]][0]
				job_id_ls.append(job_id)
		return job_id_ls
	
	def show_nodes_of_this_job(self, widget, event=None, data=None):
		"""
		2008-11-06
			fill in the treeview_nodes
			execute upon clicking the 'show nodes' in the popup menu
		"""
		if self.backend_ins is None:
			sys.stderr.write("Backend is not selected yet! Select one.\n")
			return
		
		yh_gnome.create_columns(self.treeview_nodes, self.backend_ins.display_node_label_ls)
		self.liststore_nodes = gtk.ListStore(*self.backend_ins.display_node_label_type_ls)
		job_id_ls = self.get_selected_job_id_ls()
		list_2d = self.backend_ins.returnNodeInfoGivenJobs(job_id_ls)
		yh_gnome.fill_treeview(self.treeview_nodes, self.liststore_nodes, list_2d, reorderable=True, multi_selection=True)
	
	def popup_menu_cb(self, widget, button, time, data = None):
		"""
		2008-11-06
			not used.
			learned from test/python/test_gnome_popupmenu.py. event popup_menu only happens to status icon.
		"""
		if button == 3 or button==2:
			if data:
				data.show_all()
				data.popup(None, None, None, 3, time)
	
	def rows_selected(self, treeview, event, appbar=None, menu=None, item_name='row', **keywords):
		"""
		2009-11-11
			expand it to deal with selection of all treeviews
		2008-11-05
			report in the appbar how many jobs has been selected
			adapted from pymodule/QCVisualize.py
		2008-02-12
			to update the no_of_selected rows (have to double click a row to change a cursor if it's multiple selection)
		"""
		pathlist = []
		treeview_selection = treeview.get_selection()
		treeview_selection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		no_of_items = len(pathlist)
		if no_of_items>1:
			item_name += 's'
		status_msg = "%s %s selected."%(no_of_items, item_name)
		if appbar:
			appbar.push(status_msg)
		elif self.app1_appbar1:
			self.app1_appbar1.push(status_msg)
		
		if event.button==3 and menu is not None:	#2 is middle button. 3 is right button.
			menu.show_all()
			menu.popup(None, None, None, event.button, event.time)
		#return True	#'return True' fired an event (probably button deemed pressed') to cause to the mouse to pick up the row and reorder it
	
	def showstartJob(self, widget, event=None, data=None):
		"""
		2009-2-16
			to get to know when the job will start
		"""
		pathlist = []
		self.treeselection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				job_id = self.liststore[pathlist[i][0]][0]
				showstart_output = self.backend_ins.showstartJob(job_id)
				print showstart_output.read()
	
	def displayQueueWindow(self):
		"""
		2010-5-25
			display the node_queue_app
			split out of on_menuitem_show_node_q_window_activate() to avoid looping.
		"""
		if self.backend_ins is None:
			sys.stderr.write("Backend is not selected yet! Select one.\n")
			return False	# 2009-11-10 to stop the timeout
		
		yh_gnome.create_columns(self.treeview_node_queue, self.backend_ins.display_node_label_ls)
		self.liststore_node_queue = gtk.ListStore(*self.backend_ins.display_node_label_type_ls)
		list_2d = self.backend_ins.returnQueueNodeInfo()
		yh_gnome.fill_treeview(self.treeview_node_queue, self.liststore_node_queue, list_2d, reorderable=True, multi_selection=True)
		self.node_queue_app.show_all()
	
	def on_menuitem_show_node_q_window_activate(self, widget=None, data=None):
		"""
		2010-5-25 add the timeout function to regularly check status of nodes in the queue.
		2009-11-10
			display the node_queue_app
		"""
		self.displayQueueWindow()
		# 2010-5-25 add the timeout function to regularly check status of nodes in the queue.
		self.on_button_refresh_interval_clicked()
	
	def on_button_add_node_clicked(self, widget, data=None):
		"""
		2009-11-10
			add a node to the checking queue
		"""
		pathlist = []
		self.tree_nodes_selection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				node_id = self.liststore_nodes[pathlist[i][0]][0]
				self.backend_ins.addNodeToCheckingQueue(node_id)
		
		entry_node_id = self.xml.get_widget("entry_node_id")
		self.backend_ins.addNodeToCheckingQueue(entry_node_id.get_text())
		self.displayQueueWindow()
	
	def on_button_remove_node_clicked(self, widget, data=None):
		"""
		2009-11-10
			remove the node from the checking queue
		"""
		node_queue_selection = self.treeview_node_queue.get_selection()
		pathlist = []
		node_queue_selection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				node_id = self.liststore_node_queue[pathlist[i][0]][0]
				self.backend_ins.removeNodeFromCheckingQueue(node_id)
		self.displayQueueWindow()
		
	def on_button_plot_node_status_clicked(self, widget=None, event=None, data=None):
		"""
		2009-11-10
			pop up NodeStatusWindow to show status data of a node
		"""
		node_queue_selection = self.treeview_node_queue.get_selection()
		pathlist = []
		node_id_set = set()
		node_queue_selection.selected_foreach(yh_gnome.foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				node_id = self.liststore_node_queue[pathlist[i][0]][0]
				node_id_set.add(node_id)
		
		entry_node_id = self.xml.get_widget("entry_node_id")
		node_id = entry_node_id.get_text()
		node_id_set.add(node_id)
		for node_id in node_id_set:
			data = self.backend_ins.returnNodeStatusData(node_id)
			if data.datetimeLs:
				win = NodeStatusWindow(node_id=node_id, data=data)
				win.show_all()
	
	def on_button_refresh_interval_clicked(self, widget=None, data=None):
		"""
		2009-11-10
			set the refresh interval in the node_queue_app (checking status of nodes in the queue
		"""
		entry_checking_interval = self.xml.get_widget("entry_checking_interval")
		self.backend_ins.checkStatusOfNodesInQueue()
		if self.node_queue_refresh_timeout_source_id is not None:
			gobject.source_remove(self.node_queue_refresh_timeout_source_id)
		interval_str = entry_checking_interval.get_text()
		if not interval_str:
			interval_str = '10m'	#default is 10m
		interval = self.getTimeIntervalFromStr(interval_str)
		self.node_queue_refresh_timeout_source_id = gobject.timeout_add(interval, self.backend_ins.checkStatusOfNodesInQueue)
		self.displayQueueWindow()
	
	def on_button_stop_node_status_refresh_clicked(self, widget, data=None):
		"""
		2009-11-10
			remove the timeout function
		"""
		if self.node_queue_refresh_timeout_source_id is not None:
			gobject.source_remove(self.node_queue_refresh_timeout_source_id)
		
if __name__ == '__main__':
	prog = gnome.program_init('ClusterJobManager', '0.1')
	instance = grid_job_mgr()
	gtk.main()
