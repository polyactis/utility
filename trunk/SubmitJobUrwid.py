#!/usr/bin/env python
"""
12-23-05
	A text-ui counterpart of grid_job_mgr.py.
"""

import os, sys, time
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
		self.job_edit = urwid.Edit( ('editcp',"Commands here:"), multiline=True )
		
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
			urwid.Columns(
				[
				urwid.AttrWrap( self.source_bash_profile_checkbox, 'buttn','buttnf'),
				urwid.AttrWrap( self.just_write_down_checkbox, 'buttn', 'buttnf'),
				],
				2),
			('fixed left',2), ('fixed right',2)),
		blank,
		urwid.Padding(
			urwid.Columns(
				[
				urwid.AttrWrap( self.jobname_prefix_edit, 'editbx', 'editfc' ),
				urwid.AttrWrap( self.jobnumber_edit, 'editbx', 'editfc' ),
				],
				2 ),
			('fixed left',2), ('fixed right',2) ) ,
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
		
		self.footer_text = urwid.Text("Dec 23rd, 2005 by Yu Huang")
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
	
	def submit_single_job(self, single_job):
		"""
		12-23-05
			global structures used
		"""
		job_name = self.jobname_prefix_edit.get_edit_text()
		job_name = os.path.expanduser(job_name)
		job_name = '%s_%s'%(job_name, self.jobnumber_edit.get_edit_text())
		
		jobf = open(job_name, 'w')
		jobf.write("#!/bin/sh\n")
		
		jobf.write("#PBS %s\n"%self.other_options_edit.get_edit_text())
		jobf.write("#PBS -l walltime=%s\n"%self.walltime_edit.get_edit_text())
		jobf.write("#PBS -d %s\n"%os.path.expanduser(self.workdir_edit.get_edit_text()))
		#see output while running
		if self.runtime_output_checkbox.get_state():
			jobf.write("#PBS -k eo\n")
		no_of_nodes = int(self.nodes_edit.get_edit_text())
		if no_of_nodes>0:
			jobf.write("#PBS -l nodes=%s:myri:ppn=%s\n"%(no_of_nodes, self.myri_ppn_edit.get_edit_text()))
		
		if self.source_bash_profile_checkbox.get_state():
			jobf.write("source ~/.bash_profile\n")
		
		single_job_list = single_job.split(';')
		for job_content in single_job_list:
			jobf.write("date\n")
			jobf.write('echo COMMANDLINE: %s\n'%job_content)
			jobf.write('%s\n'%job_content)
			jobf.write("date\n")
		jobf.close()
		
		qsub_output_stdout = ''
		qsub_output_stderr = ''
		if self.just_write_down_checkbox.get_state():
			qsub_output_stdout = '%s written.'%job_name
		else:
			qsub_output = os.popen3('qsub %s'%job_name)
			qsub_output_stdout = qsub_output[1].read().replace('\n', ' ')
			qsub_output_stderr = qsub_output[2].read().replace('\n', ' ')		
		return  (qsub_output_stdout, qsub_output_stderr)

if __name__ == '__main__':
	SubmitJobUrwid().main()
