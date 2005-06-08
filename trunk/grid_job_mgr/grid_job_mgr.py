#!/usr/bin/env python
"""
03-20-05
	gui program used to display jobs in the whole cluster
	given username and node_range
"""
import os, sys, pygtk, time, re, math
pygtk.require('2.0')

import gtk, gtk.glade

class node_process_data:
	"""
	03-20-05
		store the process information
	"""
	def __init__(self):
		self.label_list = []
		self.ps_output_list = []
		self.pid2index = {}
	
class node_process:
	"""
	03-20-05
		get process information given a node_range
		kill a specific process
	"""
	def __init__(self):
		self.re_pid = re.compile(r'^\ *(\d+)\ ')	#to catch the pid(first group)
	
	def get_processes(self, node_range, ps_command, username, outputfile):
		sys.stderr.write("Getting process information for %s...\n"%(username))
		node2ps = {}
		for node in node_range:
			print "node%s"%node
			#first execute
			ps_option = '-o pid,ppid,pcpu,pmem,stime,stat,time -U %s'%username
			command = 'ssh node%s %s %s > %s'%(node, ps_command, ps_option, outputfile)
			wl = ['sh', '-c', command]
			return_code = os.spawnvp(os.P_WAIT, 'sh', wl)
			if return_code!=0:	#normal exit code for ssh is 0
				sys.stderr.write("node%s sh -c ssh error code:%s, ignore.\n"%(node, return_code))
				continue
			node2ps[node] = node_process_data()	#initialize the data stu
			#first parse
			inf = open(outputfile, 'r')
			label_list = inf.readline()	#first line is label_list
			label_list = label_list.split()
			label_list.append('cmd')	#cmd is the last field
			node2ps[node].label_list = label_list
			index = 0
			for line in inf:
				process_line = line.split()
				pid = process_line[0]
				node2ps[node].pid2index[pid] = index
				node2ps[node].ps_output_list.append(process_line)
				index += 1
			inf.close()	#close the file
			
			#second execute
			ps_option = '-o pid,cmd -U %s'%username
			command = 'ssh node%s %s %s > %s'%(node, ps_command, ps_option, outputfile)
			wl = ['sh', '-c', command]
			os.spawnvp(os.P_WAIT, 'sh', wl)
			#second parse
			inf = open(outputfile, 'r')
			label_list = inf.readline()	#first line is label_list, skip
			for line in inf:
				pid = self.re_pid.match(line).groups()[0]
				cmd = self.re_pid.sub('', line[:-1])	#replace re_pid with '' and throw away '\n'
				if pid in node2ps[node].pid2index:	#new pid is probably 'ps' itself
					index = node2ps[node].pid2index[pid]
					node2ps[node].ps_output_list[index].append(cmd)	#last one is cmd
			inf.close()	#close the file
			
		sys.stderr.write("Done.\n")

		return node2ps
	
	def kill_process(self, node_number, pid):
		"""
		"""
		command = 'ssh node%s kill  -term %s'%(node_number, pid)
		wl = ['sh', '-c', command]
		os.spawnvp(os.P_WAIT, 'sh', wl)
		print "process %s on node %s killed"%(pid, node_number)
	
	def submit_jobs(self, job_list, node_range, job_fprefix, job_starting_number, time_to_run_jobs, \
		submit_option, no_of_nodes, qsub_option):
		"""
		03-21-05
			similar to codes in batch_haiyan_lam.py
		05-14-05
			correct the bug to submit sequential jobs, which are on the same line seperated by ';'
			see log_05 for detail.
		05-29-05
			add submit_option and no_of_nodes
		06-07-05
			add qsub_option
		"""
		if not time_to_run_jobs:
			time_tuple = time.localtime()
			time_to_run_jobs = "%s:%s"%(time_tuple[3], time_tuple[4]+2)
		node2jobs = {}
		for i in range(len(job_list)):
			#remainder is the node_rank
			index = int(math.fmod(i, len(node_range)))
			node = node_range[index]
			if job_list[i]:	#skip empty jobs
				if node not in node2jobs:
					node2jobs[node] = [job_list[i]]
				else:
					node2jobs[node].append(job_list[i])
		for node in node2jobs:
			#some nodes will be idle if there're more nodes than jobs
			for job in node2jobs[node]:
				job_fname = os.path.join(os.path.expanduser('~'), 'qjob/%s%s.sh'%(job_fprefix,job_starting_number))
				if os.path.isfile(job_fname):
					user_input = raw_input("File %s already exists, continue(y/N):"%job_fname)
					if user_input != 'y':
						print "job: %s not submitted"%job
						return job_starting_number
				job_f = open(job_fname, 'w')
				job_f.write("#!/bin/sh\n")
				
				#06-07-05	qsub_option
				job_f.write('#$ %s\n'%qsub_option)
				
				if no_of_nodes>1:
					job_f.write("#$ -pe mpich %s\n"%no_of_nodes)	#06-02-05	Parallel job needs >1 nodes.
				job_f.write('date\n')	#the beginning time
				for sub_job in job.split(';'):	#04-25-05	submit multiple commands on one line
					if submit_option==1:
						#'qsub' doesn't need to specify the nodes	06-01-05
						jobrow = '%s'%(sub_job)
					else:
						jobrow = 'ssh node%s %s'%(node, sub_job)
					job_f.write('echo %s\n'%jobrow)	#print the commandline
					job_f.write("%s\n"%jobrow)	#command here
				job_f.write('date\n')	#the ending time
				#close the file
				job_f.close()
				
				print "node: %s, at %s, job: %s"%(node, time_to_run_jobs, job)
				if submit_option == 1:
					jobrow = "qsub -@ ~/.qsub.options %s"%(job_fname)	#05-29-05
					os.system(jobrow)	#direct qsub doesn't work, so has to use at.
				elif submit_option == 2:
					jobrow = "echo sh %s | at -m %s"%(job_fname, time_to_run_jobs)
					os.system(jobrow)
				elif submit_option == 3:
					jobrow = 'ssh node16 "echo sh %s | at -m %s"'%(job_fname, time_to_run_jobs)
					os.system(jobrow)				
				job_starting_number+=1
		return job_starting_number
		

def foreach_cb(model, path, iter, pathlist):
	"""
	03-20-05
		for selection from a list, copied from GenePairGui_gtk.py
	"""
	pathlist.append(path)	


class grid_job_mgr:
	def __init__(self):
		"""
		05-29-05
			add submit_option and no_of_nodes
		"""
		xml = gtk.glade.XML('grid_job_mgr.glade')
		xml.signal_autoconnect(self)
		self.window1 = xml.get_widget("window1")
		self.window1.connect("destroy", self.destroy)
		self.treeview1 = xml.get_widget("treeview1")
		self.treeselection = self.treeview1.get_selection()
		
		self.entry_username = xml.get_widget("entry_username")
		self.entry_username.set_text("yuhuang")
		self.entry_node_range = xml.get_widget("entry_node_range")
		self.entry_node_range.set_text("16")
		
		self.ps_command = 'ps'
		self.tmp_fname = '/tmp/%sjobs'%(time.time())
		
		#the job submission dialog
		self.dialog_submit = xml.get_widget("dialog_submit")
		self.dialog_submit.hide()
		#how to handle the close window action?
		self.dialog_submit.connect("delete_event", self.dialog_submit_hide)
		#self.dialog_submit.connect("destroy", self.on_cancelbutton_submit_clicked)
		self.entry_node_range_submit = xml.get_widget("entry_node_range_submit")
		self.textview_submit = xml.get_widget("textview_submit")
		self.entry_job_starting_number = xml.get_widget("entry_job_starting_number")
		self.entry_time = xml.get_widget("entry_time")	#05-22-05
		self.entry_job_name_prefix = xml.get_widget("entry_job_name_prefix")	#05-22-05
		self.entry_qsub_option = xml.get_widget("entry_qsub_option")	#06-07-05
		
		self.radiobutton_qsub = xml.get_widget("radiobutton_qsub")	#05-29-05
		self.radiobutton_qsub.connect("toggled", self.on_radiobutton_submit_toggled, "radiobutton_qsub")
		self.radiobutton_at = xml.get_widget("radiobutton_at")	#05-29-05
		self.radiobutton_at.connect("toggled", self.on_radiobutton_submit_toggled, "radiobutton_at")
		self.radiobutton_at16 = xml.get_widget("radiobutton_at16")	#05-29-05
		self.radiobutton_at16.connect("toggled", self.on_radiobutton_submit_toggled, "radiobutton_at16")
		self.entry_no_of_nodes = xml.get_widget("entry_no_of_nodes")	#05-29-05
		
		self.job_starting_number = 0
		self.job_fprefix = 'grid_job_mgr'
		self.submit_option = 1	#05-29-05	default is 1(qsub), changed in on_radiobutton_qsub_toggled()
		self.submit_option_dict = {'radiobutton_qsub':1,
			'radiobutton_at':2,
			'radiobutton_at16':3}	#05-29-05	which option to take.
		
		self.node_process_instance = node_process()	#the backend class
		self.no_of_refreshes = 0
		self.tvcolumn_dict = {}
		self.cell_dict = {}
	
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
	
	def on_button_refresh_clicked(self, button_refresh):
		"""
		03-20-05
			fill in the treeview1
			--create_columns()	on first refresh
		"""
		username = self.entry_username.get_text()
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
		# set the TreeView mode to be liststore
		self.treeview1.set_model(self.liststore)

		
		for i in range(len(ps_output)+1):
			# make it searchable
			self.treeview1.set_search_column(i)
			
		# Allow drag and drop reordering of rows
		self.treeview1.set_reorderable(True)
		#setting the selection mode
		self.treeselection.set_mode(gtk.SELECTION_MULTIPLE)

		self.no_of_refreshes += 1
		
	def create_columns(self, label_list):
		"""
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
		not_sure = raw_input("Are you sure? y/n")
		if not_sure!='y':
			print "Aborted."
			return
		pathlist = []
		self.treeselection.selected_foreach(foreach_cb, pathlist)
		if len(pathlist) >0:
			for i in range(len(pathlist)):
				node_number = self.liststore[pathlist[i][0]][0]
				pid = self.liststore[pathlist[i][0]][1]
				self.node_process_instance.kill_process(node_number, pid)
		else:
			sys.stderr.write("Have you selected processes?\n")
		
	def on_button_submit_clicked(self, button_submit):
		"""
		03-21-05
			open a dialog to submit jobs
		"""
		self.entry_job_starting_number.set_text(repr(self.job_starting_number))
		self.dialog_submit.show()
		
	def on_okbutton_submit_clicked(self, okbutton_submit):
		"""
		03-21-05
			call node_process_instance to submit jobs
		05-22-05
		
		05-29-05
			add submit_option and no_of_nodes
		06-07-05
			add qsub_option
		"""
		node_range = self.entry_node_range_submit.get_text()
		if node_range:
			node_range = self.parse_node_range(node_range)
		else:
			sys.stderr.write("Which nodes to run these jobs?\n")
			return
		self.job_starting_number = self.entry_job_starting_number.get_text()
		if self.job_starting_number:
			self.job_starting_number = int(self.job_starting_number)
		else:
			sys.stderr.write("What's the job_starting_number?\n")
			return
		time_to_run_jobs = self.entry_time.get_text()	#05-22-05
		job_name_prefix = self.entry_job_name_prefix.get_text()	#05-22-05
		no_of_nodes = self.entry_no_of_nodes.get_text()	#05-29-95
		no_of_nodes = int(no_of_nodes)
		qsub_option = self.entry_qsub_option.get_text()	#06-07-05
		
		textbuffer = self.textview_submit.get_buffer()
		startiter, enditer = textbuffer.get_bounds()
		text  = textbuffer.get_text(startiter, enditer)
		job_list = text.split("\n")
		if not job_name_prefix:
			job_name_prefix = self.job_fprefix
		
		self.job_starting_number = self.node_process_instance.submit_jobs(job_list, \
			node_range, job_name_prefix, self.job_starting_number, time_to_run_jobs, \
			self.submit_option, no_of_nodes, qsub_option)
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
		"""
		real_node_range = []
		node_range = node_range.split(',')
		for nodes in node_range:
			nodes = nodes.split('-')
			nodes = map(int, nodes)
			if len(nodes)==2:
				real_node_range += range(nodes[0], nodes[1]+1)
			else:
				real_node_range += nodes
		return real_node_range
		
	def destroy(self, widget):
		if os.path.isfile(self.tmp_fname):
			try:
				os.remove(self.tmp_fname)
			except:
				sys.stderr.write("Error while removing %s.\n"%self.tmp_fname)
		gtk.main_quit()
	
	def on_radiobutton_submit_toggled(self, widget, data=None):
		"""
		05-29-05
		"""
		if widget.get_active() == 1:	#only change self.submit_option to the active radiobutton
			self.submit_option = self.submit_option_dict[data]
		

instance = grid_job_mgr()
gtk.main()
