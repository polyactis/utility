#!/usr/bin/env python
"""
2006-08-11
"""
import sys, os, math
bit_number = math.log(sys.maxint)/math.log(2)
if bit_number>40:       #64bit
	sys.path.insert(0, os.path.expanduser('~/lib64/python'))
else:   #32bit
	sys.path.insert(0, os.path.expanduser('~/lib/python'))
import os, sys, time
import urwid.curses_display
import urwid

blank = urwid.Text("")


class CheckQueue:
	palette = [
		('body','black','light gray', 'standout'),
		('reverse','light gray','black'),
		('header','white','dark red', 'bold'),
		('footer','dark cyan', 'dark blue', 'bold'),
		('error', 'dark red', 'light gray', 'bold'),
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
		self.command_edit = urwid.Edit( ('editcp',"Command  "), "qstat -u yuhuang" )
		self.display_text = urwid.Text("Text will be displayed here.\n")
		
		self.items = [
		blank,
		urwid.Padding(
			urwid.AttrWrap( self.command_edit, 'editbx', 'editfc' ), 
			('fixed left',2), ('fixed right',2)),
		blank,
		urwid.Padding(
			self.display_text,
			('fixed left',2), ('fixed right',2)),
		blank			
		]
		
		self.listbox = urwid.ListBox( self.items )
		
		instruct = urwid.Text("F6 to execute command, F7 to clusterusage. F8 to QueueInfo.py, F12 to quit.")
		header = urwid.AttrWrap( instruct, 'header' )
		
		self.footer_text = urwid.Text("Aug 11th, 2006 by Yu Huang")
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
				elif k == 'f6':
					command_line = self.command_edit.get_edit_text()
					self.run_command(command_line)
				elif k == 'f7':
					self.run_command('clusterusage')
				elif k == 'f8':
					self.run_command(os.path.expanduser('~/script/utility/QueueInfo.py'))
				elif k == 'f12':
					return	#break is not enough
				self.top_frame.keypress( size, k )
	
	def run_command(self, command_line):
		command_output_handler = os.popen3(command_line)
		command_stdout = command_output_handler[1].read()
		command_stderr = command_output_handler[2].read()
		self.display_text.set_text([('important', "%s\n"%time.asctime()), command_stdout, ('error', command_stderr)])
	

if __name__ == '__main__':
	CheckQueue().main()
