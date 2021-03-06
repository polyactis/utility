#!/usr/bin/env python
"""
12-23-05
	A text-ui counterpart of grid_job_mgr.py.
"""
import sys, os, math
bit_number = math.log(sys.maxint)/math.log(2)
if bit_number>40:       #64bit
	sys.path.insert(0, os.path.expanduser('~/lib64/python'))
	sys.path.insert(0, os.path.join(os.path.expanduser('~/script64/annot/bin')))
else:   #32bit
	sys.path.insert(0, os.path.expanduser('~/lib/python'))
	sys.path.insert(0, os.path.join(os.path.expanduser('~/script/annot/bin')))
import os, sys, time, subprocess
import urwid.curses_display
import urwid

blank = urwid.Text("")

class SubmitJobUrwid:
	palette = [
		('body','black','light gray', 'standout'),
		('reverse','light gray','black'),
		('header','white','dark red', 'bold'),
		('footer','dark cyan', 'dark blue', 'bold'),
		('important','dark blue','light gray',('standout','underline')),
		('editfc','white', 'dark blue', 'bold'),
		('editbx','light gray', 'dark blue'),
		('editcp','black','light gray', 'standout'),
		('bright','dark gray','light gray', ('bold','standout')),
		('buttn','black','dark cyan'),
		('buttnf','white','dark blue','bold'),
		]
	
	def __init__(self):
		"""
		01-04-06 add job_content_reset_button and its callback
			use GridFlow instead of Pile
		"""
		self.walltime_edit = urwid.Edit( ('editcp',"walltime="), "200:00:00" )
		self.nodes_edit = urwid.IntEdit( ('editcp', "nodes="), 0 )
		self.myri_ppn_edit = urwid.IntEdit( ('editcp', "myri:ppn="), 4)
		self.workdir_edit = urwid.Edit( ("editcp",  "WORKDIR(-d) "), '~/qjob_output')
		self.runtime_output_checkbox = urwid.CheckBox("See output while running")
		self.other_options_edit = urwid.Edit( ("editcp", "others:"), '-q cmb -j oe -S /bin/bash')
		self.source_bash_profile_checkbox = urwid.CheckBox("source ~/.bash_profile")
		self.source_bash_profile_checkbox.set_state(True)
		self.just_write_down_checkbox = urwid.CheckBox("Write jobfile. No submission.")
		self.jobname_prefix_edit = urwid.Edit( ("editcp", "jobname_prefix:"), '~/qjob/job')
		self.jobnumber_edit = urwid.IntEdit( ("editcp", "job number:"), 0)
		self.job_content_reset_button = urwid.Button("Job Content Reset", self.job_content_reset)
		self.exit_button = urwid.Button("Exit", self.program_exit)
		self.job_edit = urwid.Edit( ('editcp',""), multiline=True )
		
		self.items = [
		urwid.Padding(
			urwid.Columns(
				[
				urwid.AttrWrap( self.walltime_edit, 'editbx', 'editfc' ),
				urwid.AttrWrap( self.nodes_edit, 'editbx', 'editfc'),
				urwid.AttrWrap( self.myri_ppn_edit, 'editbx', 'editfc'),
				],
				2 ), 
			('fixed left',2), ('fixed right',2)),
		blank,
		urwid.Padding(
			urwid.Columns(
				[
				urwid.AttrWrap( self.workdir_edit, 'editbx', 'editfc' ), 
				urwid.AttrWrap( self.runtime_output_checkbox, 'buttn', 'buttnf'),
				],
				2),
			('fixed left',2), ('fixed right',2)),
		blank,
		urwid.Padding(
			urwid.AttrWrap( self.other_options_edit, 'editbx', 'editfc' ), ('fixed left',2), ('fixed right',2)),
		blank,
		urwid.Padding(
			urwid.GridFlow(
				[
				urwid.AttrWrap( self.source_bash_profile_checkbox, 'buttn','buttnf'),
				urwid.AttrWrap( self.just_write_down_checkbox, 'buttn', 'buttnf'),
				urwid.AttrWrap( self.jobname_prefix_edit, 'editbx', 'editfc' ),
				urwid.AttrWrap( self.jobnumber_edit, 'editbx', 'editfc' ),
				urwid.AttrWrap(self.job_content_reset_button, 'buttn', 'buttnf'),
				urwid.AttrWrap(self.exit_button, 'buttn', 'buttnf'),
				],
				34, 2, 1, 'left'),
			('fixed left',2), ('fixed right',2)),
		blank,
		urwid.Padding(
			urwid.Pile(
			[
			urwid.Text('One line one job. One job with >1 commands put on one line, separated by ;'),
			urwid.AttrWrap(self.job_edit, 'editbx', 'editfc'),
			], 1),
			('fixed left',2), ('fixed right',2) )
			
		]
		
		self.listbox = urwid.ListBox( self.items )
		
		instruct = urwid.Text("Job submission program based on Urwid. F8 to submit, F12 to quit.")
		header = urwid.AttrWrap( instruct, 'header' )
		
		self.footer_text = urwid.Text("Mar 15th, 2008 by Yu Huang")
		footer = urwid.AttrWrap(self.footer_text, 'footer')
		
		self.top_frame = urwid.Frame(urwid.AttrWrap(self.listbox, 'body'), header, footer)
	
	
	def main(self):
		self.ui = urwid.curses_display.Screen()
		self.ui.register_palette( self.palette )
		self.ui.run_wrapper( self.run )
	
	def run(self):
		size = self.ui.get_cols_rows()
		
		while True:
			canvas = self.top_frame.render( size, focus=1 )
			self.ui.draw_screen( size, canvas )
			keys = self.ui.get_input()
			for k in keys:
				if k == "window resize":
					size = self.ui.get_cols_rows()
					continue
				elif k == 'f8':
					self.submit_jobs()
				elif k == 'f12':
					return	#break is not enough
				self.top_frame.keypress( size, k )
	
	def submit_jobs(self):
		jobs = self.job_edit.get_edit_text()
		if jobs:
			job_list = jobs.split('\n')
			for single_job in job_list:
				qsub_output_stdout, qsub_output_stderr = self.submit_single_job(single_job)
				if qsub_output_stderr:
					self.footer_text.set_text(('header', "Failed: %s"%qsub_output_stderr))
				else:
					current_job_number = int(self.jobnumber_edit.get_edit_text())
					current_job_number += 1
					self.jobnumber_edit.set_edit_text('%s'%current_job_number)
					self.footer_text.set_text("%s"%qsub_output_stdout)
		else:
			self.footer_text.set_text(('header', "Empty job!!"))
	
	def write_job_to_file(cls, job_content, job_fname, no_of_nodes, \
				qsub_option, ppn=None, workdir=None, walltime=None, runtime_output_stdout=False,\
				source_bash=True):
		"""
		2008-11-04
			return the content of the job
		2008-11-03
			refactor submit_single_job() to allow hpc_cmb_pbs.py to be able to call this function
		"""
		content_lines = "#!/bin/sh\n"
		
		if qsub_option:
			content_lines+="#PBS %s\n"%qsub_option
		if walltime:
			content_lines+="#PBS -l walltime=%s\n"%walltime
		if workdir:
			content_lines+="#PBS -d %s\n"%workdir
		#see output while running
		if runtime_output_stdout:
			content_lines+="#PBS -k eo\n"
		
		if no_of_nodes>0 and ppn:
			content_lines+="#PBS -l nodes=%s:myri:ppn=%s\n"%(no_of_nodes, ppn)
		
		if source_bash:
			content_lines+="source ~/.bash_profile\n"
		
		content_lines+="date\n"
		content_lines+='echo COMMANDLINE: "%s"\n'%job_content
		content_lines+='%s\n'%job_content
		content_lines+="date\n"
		
		jobf = open(job_fname, 'w')
		
		jobf.write(content_lines)
		jobf.close()
		return content_lines
	
	write_job_to_file = classmethod(write_job_to_file)
	
	def submit_single_job(self, single_job):
		"""
		2008-11-03
			no longer split a single_job into several lines by ';'
			subprocess.Popen() replaces os.popen3()
		12-23-05
			global structures used
		"""
		job_name = self.jobname_prefix_edit.get_edit_text()
		job_name = os.path.expanduser(job_name)
		job_name = '%s_%s'%(job_name, self.jobnumber_edit.get_edit_text())
		
		qsub_option = self.other_options_edit.get_edit_text()
		walltime = self.walltime_edit.get_edit_text()
		workdir = os.path.expanduser(self.workdir_edit.get_edit_text())
		runtime_output_stdout= self.runtime_output_checkbox.get_state()
		no_of_nodes = int(self.nodes_edit.get_edit_text())
		ppn = self.myri_ppn_edit.get_edit_text()
		source_bash = self.source_bash_profile_checkbox.get_state()
		
		return_code = self.write_job_to_file(single_job, job_name, no_of_nodes, \
				qsub_option, ppn=ppn, workdir=workdir, walltime=walltime, runtime_output_stdout=runtime_output_stdout,\
				source_bash=source_bash)
		
		qsub_output_stdout = ''
		qsub_output_stderr = ''
		if self.just_write_down_checkbox.get_state():
			qsub_output_stdout = '%s written.'%job_name
		else:
			command_handler = subprocess.Popen('qsub %s'%job_name, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)	#2008-10-20 shell=True allows command_line to be a string, rather than a list of command and its arguments
			qsub_output_stdout = command_handler.stdout.read().replace('\n', ' ')
			qsub_output_stderr = command_handler.stderr.read().replace('\n', ' ')
			"""
			qsub_output = os.popen3('qsub %s'%job_name)
			qsub_output_stdout = qsub_output[1].read().replace('\n', ' ')
			qsub_output_stderr = qsub_output[2].read().replace('\n', ' ')
			"""
		return  (qsub_output_stdout, qsub_output_stderr)
		
	def job_content_reset(self, button_object):
		"""
		01-04-06
			clear the content in job_edit
		"""
		self.job_edit.set_edit_text('')
	
	def program_exit(self, button_object):
		"""
		2008-03-15
			exits the program. F12 doesn't work everywhere.
		"""
		sys.exit(0)

		
if __name__ == '__main__':
	SubmitJobUrwid().main()
