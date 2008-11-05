#!/usr/bin/env python
"""
Examples:
	#update information of jobs (not nodes) in database
	hpc_cmb_pbs.py -u crocea

Description:
	2008-11-01
		backend for grid_job_mgr.py to communicate with hpc-cmb cluster
		
		usually it doesn't run standalone.
"""
import sys, os, math
#bit_number = math.log(sys.maxint)/math.log(2)
#if bit_number>40:       #64bit
sys.path.insert(0, os.path.expanduser('~/lib/python'))
sys.path.insert(0, os.path.join(os.path.expanduser('~/script/')))
import JobDB
import subprocess, time, traceback

import StringIO, re
from sets import Set
from pymodule import PassingData
from datetime import datetime, timedelta
from utility.SubmitJobUrwid import SubmitJobUrwid

class hpc_cmb_pbs(object):
	__doc__ = __doc__
	option_default_dict = {('drivername', 1,):['postgres', 'v', 1, 'which type of database? mysql or postgres', ],\
							('hostname', 1, ): ['localhost', 'z', 1, 'hostname of the db server', ],\
							('dbname', 1, ): ['graphdb', 'd', 1, 'database name', ],\
							('schema', 0, ): ['cluster_job', 'k', 1, 'database schema name', ],\
							('db_user', 1, ): [None, 'u', 1, 'database username', ],\
							('db_passwd', 0, ): [None, 'p', 1, 'database password', ],\
							('job_file_dir', 0, ):['~/qjob/', '', 0, 'which directory job files are stored'],\
							('workdir', 0, ):['~/qjob_output/', '', 0, 'which directory the file of job stdout/err is stored'],\
							('cluster_username', 0, ):['yuhuang', '', 0, 'username on the cluster'],\
							('cluster_main_node', 0, ):['hpc-cmb.usc.edu', '', 0, 'hostname of the cluster'],\
							('queue_id', 0, ):['cmb', '', 0, 'the queue to check'],\
							('commit', 0, int):[0, 'c', 0, 'commit the db operation. this commit happens after every db operation, not wait till the end.'],\
							('debug', 0, int):[0, 'b', 0, 'toggle debug mode'],\
							('report', 0, int):[0, 'r', 0, 'toggle report, more verbose stdout/stderr.'],\
							}
	def __init__(self,  **keywords):
		from pymodule import ProcessOptions
		self.ad = ProcessOptions.process_function_arguments(keywords, self.option_default_dict, error_doc=self.__doc__, class_to_have_attr=self)
		
		self.current_job_id = None
		self.cluster_main_node = 'hpc-cmb.usc.edu'
		self.queue_id = 'cmb'
		self.db = JobDB.ClusterJobDB(drivername=self.drivername, username=self.db_user,
				   password=self.db_passwd, hostname=self.hostname, database=self.dbname, schema=self.schema)
		self.db.setup(create_tables=False)
		self.pbsnode_key = re.compile(r'^[\t ]*(?P<key>.*?) = ')
			#12-20-05	? is used to do non-greedy match, for i.e.: '!Platform_description = Keywords = high density oligonucleotide array'
		self.pbsnode_value = re.compile(r' = (?P<value>.*)$')
		
		self.mem_pattern_from_qsub_option = re.compile(r'-l mem=(?P<memory>\w*)')
		self.tmp_fname = '/tmp/job_%s'%(time.time())
		self.getJobIDFromFullName = lambda x: int(x.split('.')[0])
		
	def submit_job(self, job_content, job_file_dir, job_fprefix, job_starting_number, no_of_nodes, \
				qsub_option, ppn=None, submit_option=None, workdir=None, walltime=None, runtime_output_stdout=False,\
				queue='cmb'):
		"""
		2008-11-04
			if job_content contains '#!/bin/sh', just write the contents into files and submit
			
			else:
				treat each line as a job, submit, create a job in db
		"""
		job_content_io = StringIO.StringIO(job_content)
		
		if job_content.find('#!/bin/sh')!=-1:	#won't save the job in db. fetch its from 'qstat -f job_id' by clicking 'refresh'
			single_job = ''
			for line in job_content_io:
				if line.find('#!/bin/sh')!=-1:
					if single_job:	#to skip the first '#!/bin/sh'
						of = open(self.tmp_fname, 'w')
						of.write(single_job)
						of.close()
						job_fname = os.path.join(job_file_dir, '%s%s'%(job_fprefix, job_starting_number))
						job_id = self.qsub_job_on_cluster(self.tmp_fname, job_fname)
						job_starting_number += 1
					single_job = line	#set to the beginning line
				else:
					single_job += line
				
			if single_job:	#the last job
				of = open(self.tmp_fname, 'w')
				of.write(single_job)
				of.close()
				job_fname = os.path.join(job_file_dir, '%s%s'%(job_fprefix, job_starting_number))
				job_id = self.qsub_job_on_cluster(self.tmp_fname, job_fname)
				job_starting_number += 1
				single_job = ''
		else:	#2008-11-03 de-novo submission
			for single_job in job_content_io:	#each line is a job
				return_code = SubmitJobUrwid.write_job_to_file(single_job, self.tmp_fname, no_of_nodes, \
															qsub_option, ppn=ppn, workdir=workdir, \
															walltime=walltime, runtime_output_stdout=runtime_output_stdout)
				job_basename = '%s%s'%(job_fprefix, job_starting_number)
				job_fname = os.path.join(job_file_dir, job_basename)
				job_id = self.qsub_job_on_cluster(self.tmp_fname, job_fname)
				if job_id is not None:
					#save the job in db
					job = JobDB.Job(id=job_id)
					job.short_name = job_basename
					state_id = 'Q'
					job_state = JobDB.JobState.get(state_id)
					if not job_state:
						job_state = JobDB.JobState(short_name=state_id)
					job.job_state = job_state
					job.job_fname = job_fname
					job.content = single_job
					
					job.username = self.cluster_username
					job.queue_id = queue
					job.no_of_nodes = no_of_nodes
					job.ppn = ppn
					mem_pattern = self.mem_pattern_from_qsub_option.search(qsub_option)	#find the memory from qsub_option
					if mem_pattern:
						job.memory = mem_pattern.group('memory')
					job.walltime = walltime
					job.workdir = workdir
					job.see_output_while_running = runtime_output_stdout
					if job.see_output_while_running:	#in home dir
						stdout_dir = '~'
					else:	#in workdir
						stdout_dir = workdir
					
					job.job_stdout_fname=os.path.join(stdout_dir, '%s.o%s'%(job_basename, job_id))
					if qsub_option.find('-j oe')==-1 and qsub_option.find('-j eo')==-1:	#need to set the job_stderr_fname as well
						job.job_stderr_fname=os.path.join(stdout_dir,  '%s.e%s'%(job_basename, job_id))
						
					job.time_submitted = datetime.now()		
					self.db.session.save_or_update(job)
					self.db.session.flush()
				
				job_starting_number += 1
				
		return job_starting_number
	
	def qsub_job_on_cluster(self, source_fname, target_fname):
		"""
		2008-11-04
			1. scp file over to the cluster
			2. qsub on the cluster
			3. get job_id and return it
		"""
		commandline = 'scp %s %s@%s:%s'%(source_fname, self.cluster_username, self.cluster_main_node, target_fname)
		command_out = self.runLocalCommand(commandline, report_stdout=True)
		commandline = 'qsub "%s"'%target_fname	#2008-11-04 double quotes escape the '~'. otherwise '~' would become local home dir
		command_out = self.runRemoteCommand(commandline, report_stdout=True)
		if command_out.stdout_content:
			job_id = self.getJobIDFromFullName(command_out.stdout_content)
		else:
			job_id = None
		return job_id
	
	def kill_job(self, job_id):
		"""
		2008-11-01
		"""
		commandline = 'qdel %s'%(job_id)
		command_out = self.runRemoteCommand(commandline)
		job = JobDB.Job.get(job_id)
		if command_out.output_stdout:
			sys.stderr.write("qdel stdout: %s.\n"%( command_out.output_stdout.read()))
		sys.stderr.write("Job %s (%s) killed.\n"%(job_id, job.short_name))
		
	def get_job(self, job_id):
		"""
		2008-11-01
		"""
		job = JobDB.Job.get(job_id)
		return job
	
	def get_job_id_ls_from_queue(self, username=None):
		"""
		2008-11-01
		"""
		commandline = 'qstat %s'%(self.queue_id)
		if username:
			commandline += ' -u %s'%username
		command_out = self.runRemoteCommand(commandline)
		
		job_id_ls = []
		if command_out.output_stdout:
			#output_stdout = StringIO.StringIO(command_out.output_stdout)
			for i in range(5):	#skip 5 header lines
				command_out.output_stdout.next()
			
			for line in command_out.output_stdout:
				job_id = line.split()[0]	#like '4670208.hpc-pbs.usc.'
				job_id = int(job_id.split('.')[0])	#take 4670208
				job_id_ls.append(job_id)
		return job_id_ls
	
	
	node_var_name_set = Set(['arch', 'opsys', 'uname', 'totmem', 'physmem', 'size', 'rectime'])
	nodelog_var_name_set = Set(['nsessions', 'nusers', 'idletime', 'availmem', 'loadave', 'netload', 'size', 'rectime'])
	def getNodeStatus(self, node, status_value, node_state, further_check_node):
		"""
		2008-11-01
			parse the 'status' property of a node info in pbsnodes output
			
			
			hpc2269
			     state = free
			     np = 8
			     properties = pe1950,myri,P4,quadcore,x86_64,disk60g,m10g,cmb
			     ntype = cluster
			     jobs = 1/4764755.hpc-pbs.usc.edu, 2/4764756.hpc-pbs.usc.edu, 3/4764970.hpc-pbs.usc.edu, 4/4766314.hpc-
			pbs.usc.edu, 5/4764757.hpc-pbs.usc.edu, 6/4764759.hpc-pbs.usc.edu, 7/4764760.hpc-pbs.usc.edu
			     status = arch=x86_64,opsys=linux,uname=Linux hpc2269 2.6.9-78.0.5.ELsmp #1 SMP Wed Oct 8 07:06:30 EDT 
			2008 x86_64,sessions=10531 10583 10635 10687 10739 11270 13973,nsessions=7,nusers=2,idletime=195243,totmem=
			13350464kb,availmem=3682348kb,physmem=12298248kb,ncpus=8,loadave=7.00,netload=117318985598,size=68967008kb:
			69053492kb,state=free,jobs=4764755.hpc-pbs.usc.edu 4764756.hpc-pbs.usc.edu 4764757.hpc-pbs.usc.edu 4764759.
			hpc-pbs.usc.edu 4764760.hpc-pbs.usc.edu 4764970.hpc-pbs.usc.edu 4766314.hpc-pbs.usc.edu,rectime=1225610468


		"""
		
		job_id_ls = []
		status_list = status_value.split(',')
		nodelog = None
		for status_item in status_list:
			status_item_key, status_item_value = status_item.split('=')
			if status_item_key=='jobs':
				if further_check_node:	#don't check it if not further_check_node (no username's jobs on this node)
					job_id_ls = status_item_value.split(' ')
					job_id_ls = map(self.getJobIDFromFullName, job_id_ls)
			elif status_item_key in self.node_var_name_set:
				setattr(node, status_item_key, status_item_value)
			elif status_item_key in self.nodelog_var_name_set:
				if further_check_node:
					if nodelog is None:
						nodelog = JobDB.NodeLog()
						nodelog.node = node
						nodelog.state = node_state
					setattr(nodelog, status_item_key, status_item_value)
		
		return job_id_ls, nodelog
	
	def updateOneNode(self, block, job_id_set, job_id2current_nodelog_id_ls):
		"""
		2008-11-01
			1. store the node into Node if it's new
			2. store node status info into NodeLog if it has a job in job_id_set
			3. 
		
		
			a typical segment looks like this
			
			hpc2236
			     state = job-exclusive
			     np = 8
			     properties = pe1950,myri,P4,quadcore,x86_64,disk60g,m10g,cmb
			     ntype = cluster
			     jobs = 0/4759956.hpc-pbs.usc.edu, 1/4759956.hpc-pbs.usc.edu, 2/4759956.hpc-pbs.usc.edu, 3/4759956.hpc-
			pbs.usc.edu, 4/4759956.hpc-pbs.usc.edu, 5/4759956.hpc-pbs.usc.edu, 6/4759956.hpc-pbs.usc.edu, 7/4759956.hpc
			-pbs.usc.edu
			     status = arch=x86_64,opsys=linux,uname=Linux hpc2236 2.6.9-78.0.5.ELsmp #1 SMP Wed Oct 8 07:06:30 EDT 
			2008 x86_64,sessions=23292 23442 23443 23444 23445 23446 23447 23448 23449,nsessions=9,nusers=1,idletime=89
			6593,totmem=13350464kb,availmem=7998652kb,physmem=12298248kb,ncpus=8,loadave=1.14,netload=1172062039,size=6
			8967252kb:69053492kb,state=free,jobs=4759956.hpc-pbs.usc.edu,rectime=1225674376
			     note = E2119 error. Jobs running (07/11)
		"""
		machine_id = block[0].strip()
		if self.debug:
			sys.stderr.write("Checking node %s ... \n"%machine_id)
		arch = None
		np = None
		queue_list = None
		status_value = None
		bad_machine_id = None
		
		node = JobDB.Node.get(machine_id)
		if not node:	#not in db yet
			node = JobDB.Node(short_name=machine_id)
		further_check_node = 0
		for line in block:
			p_key_result = self.pbsnode_key.search(line)
			p_value_result = self.pbsnode_value.search(line)
			if p_key_result and p_value_result:
				key = p_key_result.group('key')
				value = p_value_result.group('value')
				if key == 'np':
					np = int(value)
					if node.date_created is None:
						node.ncpus = np
				elif key == 'properties':
					queue_list = value.split(',')
					if node.date_created is None:	#do it only when the node is new in the database
						for queue_name in queue_list:
							node_prop = JobDB.NodeProperty.get(queue_name)
							if not node_prop:
								node_prop = JobDB.NodeProperty(short_name=queue_name)
							node2property = JobDB.Node2Property(node=node, property=node_prop)
							
							node.properties.append(node2property)
				elif key == 'jobs':
					job_list = value.split(', ')
					for job in job_list:
						cpu_no, job_id = job.split('/')
						job_id = int(job_id.split('.')[0])
						if job_id in job_id_set:
							further_check_node=1
				elif key == 'status':
					status_value = value
				elif key =='state':
					node_state = value
		
		
		if (further_check_node or getattr(node, 'date_created', None) is None) and status_value:
			job_id_ls, nodelog = self.getNodeStatus(node, status_value, node_state, further_check_node)
			self.db.session.save_or_update(node)
			if further_check_node and nodelog:	#don't check it if not further_check_node (no username's jobs on this node)
				self.db.session.save_or_update(nodelog)
				self.db.session.flush()
				for job_id in job_id_ls:
					job_id2current_nodelog_id_ls[job_id] = nodelog.id
		
		elif node.ncpus and node.arch:
			self.db.session.save_or_update(node)
		else:
			bad_machine_id = machine_id
		
		self.db.session.flush()
		if self.debug:
			sys.stderr.write("Done.\n")
		return bad_machine_id
	
	def updateNodeInDB(self, job_id_set):
		"""
		2008-11-01
			run 'pbsnodes -a' to check node information
		
		"""
		if self.debug:
			sys.stderr.write("Updating node information in db ... \n")
		commandline = 'pbsnodes -a'
		command_out = self.runRemoteCommand(commandline, report_stderr=False)
		
		job_id2current_nodelog_id_ls = {}
		block = []
		bad_machine_id_ls = []
		for line in command_out.output_stdout:
			if line == '\n':
				bad_machine_id = self.updateOneNode(block, job_id_set, job_id2current_nodelog_id_ls)
				if bad_machine_id:
					sys.stderr.write("Node %s is bad.\n"%bad_machine_id)
					bad_machine_id_ls.append(bad_machine_id)
				block = []
				continue
			else:
				block.append(line)
		
		if self.debug:
			sys.stderr.write("Done.\n")
		return job_id2current_nodelog_id_ls
	
	def getJobKey2Value(self, qstat_output):
		"""
		2008-11-01
			qstat_output looks like this:
			
			Job Id: 4771477.hpc-pbs.usc.edu
			    Job_Name = test
			    Job_Owner = yuhuang@hpc-cmb.usc.edu
			    job_state = R
			    queue = cmb
			    server = hpc-pbs.usc.edu
			    Checkpoint = u
			    ctime = Sun Nov  2 03:06:22 2008
			    Error_Path = hpc-cmb.usc.edu:/auto/cmb-01/yuhuang/qjob_output/test.e477147
				7
			    exec_host = hpc2289/4+hpc2289/3+hpc2289/2+hpc2289/1
			    Hold_Types = n
			    Join_Path = oe
			    Keep_Files = n
			    Mail_Points = a
			    mtime = Sun Nov  2 03:06:25 2008
			    Output_Path = hpc-cmb.usc.edu:/auto/cmb-01/yuhuang/qjob_output/test.o47714
				77
			    Priority = 0
			    qtime = Sun Nov  2 03:06:22 2008
			    Rerunable = True
			    Resource_List.nodect = 1
			    Resource_List.nodes = 1:myri:ppn=4
			    Resource_List.walltime = 200:00:00
			    session_id = 32041
			    Shell_Path_List = /bin/bash

		"""
		job_key2value = {}
		old_key = None
		for line in qstat_output:
			p_key_result = self.pbsnode_key.search(line)
			p_value_result = self.pbsnode_value.search(line)
			if p_key_result and p_value_result:
				new_key = p_key_result.group('key')
				value = p_value_result.group('value')
				job_key2value[new_key] = value.strip()
				old_key = new_key
			else:
				if old_key:
					job_key2value[old_key] += line.strip()	#add the broken lines
		return job_key2value
	
	def fillInNewJob(self, job, job_key2value, username):
		"""
		2008-11-04
			fix bugs when some fields in job_key2value are not available
		2008-11-01
			fill in via job_key2value
			
		"""
		if self.debug:
			sys.stderr.write("Filling in a new job %s ... \n"%job.id)
		job.short_name = job_key2value['Job_Name']
		state_id = job_key2value['job_state']
		job_state = JobDB.JobState.get(state_id)
		if not job_state:
			job_state = JobDB.JobState(short_name=state_id)
		job.job_state = job_state
		
		job.job_fname = os.path.join(self.job_file_dir, job_key2value['Job_Name'])
		job.content = self.fetch_file_content(job.job_fname)
		
		if job_key2value['Keep_Files']=='n':
			job.see_output_while_running = False
		elif job_key2value['Keep_Files']=='eo' or job_key2value['Keep_Files']=='oe':
			job.see_output_while_running = True
		
		output_path = job_key2value.get('Output_Path')
		if output_path:
			output_path_ls = output_path.split(':')
			if len(output_path_ls)>1:
				stdout_fname = output_path_ls[1]
				
				if job.see_output_while_running:
					job.job_stdout_fname = '~/%s'%os.path.basename(stdout_fname)
				else:
					job.job_stdout_fname = stdout_fname
				job.job_stdout = self.fetch_file_content(job.job_stdout_fname)
		else:
			job.job_stdout_fname = output_path
			
		if job_key2value['Join_Path']=='n':	#path not merged
			error_path = job_key2value.get('Error_Path')
			if error_path:
				error_path_ls = error_path.split(':')
				if len(error_path_ls)>1:
					stderr_fname = error_path_ls[1]
					if job.see_output_while_running:
						job.job_stderr_fname = '~/%s'%os.path.basename(stderr_fname)
					else:
						job.job_stderr_fname = stderr_fname
					job_stderr = self.fetch_file_content(job.job_stderr_fname)
				else:
					job.job_stderr_fname = error_path
		
		job.username = username
		
		job.queue_id = job_key2value['queue']
		
		job.server = job_key2value['server']
		job.no_of_nodes = int(job_key2value.get('Resource_List.nodect'))
		job.ppn = int(job_key2value['Resource_List.nodes'].split('=')[-1])
		job.memory = job_key2value.get('Resource_List.mem')
		job.walltime = job_key2value.get('Resource_List.walltime')
		job.workdir = self.workdir
		job.time_submitted = datetime.strptime(job_key2value['ctime'], '%a %b %d %H:%M:%S %Y')	#ctime ~ 'Fri Oct 31 13:06:14 2008'
		job.time_started = datetime.strptime(job_key2value['mtime'], '%a %b %d %H:%M:%S %Y')	#not sure it's ctime/mtime/qtime/etime. but mtime is always minutes bigger than others
		
		if job.state_id=='C':	#job is done, very rare to happen right when executing 'qstat -f job_id'
			job.time_finished = datetime.now()
		
		exec_host = job_key2value.get('exec_host')
		if exec_host:	#2008-11-04 it could be none if it's not running
			node_cpus = exec_host.split('+')
			node_set = Set()
			for node_cpu in node_cpus:
				node_name, cpu = node_cpu.split('/')
				node = JobDB.Node.get(node_name)
				if node and node_name not in node_set:
					node_set.add(node_name)
					job2node = JobDB.Job2Node(node=node, job=job)
					job.nodes.append(job2node)
		
		#for nodelog_id in current_nodelog_id_ls:
		if self.debug:
			sys.stderr.write("Done.\n")
	
	def updateOneJobInDB(self, job_id, username):
		"""
		2008-11-04
			if it's a running job and already in db, update job_stdout only when job_stdout_fname has changed since the last time its info in db was updated.
			ditto for job_stderr
		2008-11-01
			use 'qstat -f $job_id' to get full info of a job
			
			if 'qstat -f' fails,
				mark the job completed and fill in time_finished
				
				grab its stdout/stderr info from corresponding file and stuff it into db
			else:
				update JobLog
				if a job is not in db yet, grab its content from remote host
				
				
				elif a job is in db:
					update its state_id
					update job_stdout
					update job_stderr

		"""
		if self.debug:
			sys.stderr.write("Checking job %s ... \n"%job_id)
		job = JobDB.Job.get(job_id)
		if not job:
			job = JobDB.Job(id=job_id)
		elif job.state_id=='C':	#job in db and complete. do nothing and return
			return
		is_job_completed = 0
		commandline = 'qstat -f %s'%(job_id)
		command_out = self.runRemoteCommand(commandline)
		output_stdout = command_out.output_stdout.read()
		if output_stdout and not command_out.stderr_content:	#2008-11-02 this job still exists and no error
			job_key2value = self.getJobKey2Value(StringIO.StringIO(output_stdout))
			if job.short_name is None:	#not in db yet
				self.fillInNewJob(job, job_key2value, username)
			else:
				if job.state_id!= job_key2value['job_state']:
					job.state_id = job_key2value['job_state']
					if job.state_id=='C':
						job.time_finished = datetime.now()
				if job.see_output_while_running:
					time_of_last_change = self.getRemoteFileTimeOfLastChange(job.job_stdout_fname)
					if time_of_last_change:
						time_of_last_change = time_of_last_change + timedelta(minutes=5)	#2008-11-04 allow 5 minutes behind on the cluster
						if (not job.date_updated and job.date_created and job.date_created<time_of_last_change) or \
									(job.date_updated and job.date_updated<time_of_last_change):	#condition to fetch the content
							job.job_stdout = self.fetch_file_content(job.job_stdout_fname)
					if job_key2value['Keep_Files']=='n' or job.job_stderr_fname:	#it might not be available if it's merged into stdout
						time_of_last_change = self.getRemoteFileTimeOfLastChange(job.job_stderr_fname)
						if time_of_last_change:
							time_of_last_change = time_of_last_change + timedelta(minutes=5)	#2008-11-04 allow 5 minutes behind on the cluster
							if (not job.date_updated and job.date_created and job.date_created<time_of_last_change) or \
									(job.date_updated and job.date_updated<time_of_last_change):	#condition to fetch the content
								job.job_stderr = self.fetch_file_content(job.job_stderr_fname)
			
			job_log = JobDB.JobLog()
			job_log.job = job
			job_log.cput = job_key2value.get('resources_used.cput')
			job_log.mem_used = job_key2value.get('resources_used.mem')
			job_log.vmem_used = job_key2value.get('resources_used.vmem')
			job_log.walltime_used = job_key2value.get('resources_used.walltime')
			
			self.db.session.save_or_update(job_log)
		else:
			is_job_completed = 1
		
		if is_job_completed and job.short_name is not None:	#2008-11-02 it's completed. but not recorded in db
			job.state_id='C'
			job.job_stdout = self.fetch_file_content(job.job_stdout_fname)	#fetch the content no matter what has changed
			job.time_finished = self.parseTimeFinishedOutOfStdoutFile(job.job_stdout)
			if job.job_stderr_fname:
				job.job_stderr = self.fetch_file_content(job.job_stderr_fname)
		if job.short_name is not None:
			self.db.session.save_or_update(job)
		
		self.db.session.flush()
		if self.debug:
			sys.stderr.write("Done.\n")
	
	
	def getRemoteFileTimeOfLastChange(self, fname):
		"""
		2008-11-04
			use stat to figure out the time of last change of a file on remote cluster
		"""
		commandline = 'stat -c %z ' + '"%s"'%fname	#%x, %y, %z are time of last access, modification, change respectivelys
		#output looks like "2008-11-04 00:22:18.000000000 -0800"
		command_out = self.runRemoteCommand(commandline)
		stdout_content = command_out.output_stdout.read()
		try:
			time_of_last_change = datetime.strptime(stdout_content[:19].strip(), '%Y-%m-%d %H:%M:%S')	#.%f %z for microseconds and timezone info doesn't work in python 2.5
		except:
			traceback.print_exc()
			sys.stderr.write('%s.\n'%repr(sys.exc_info()))
			time_of_last_change = None
		return time_of_last_change
	
	def parseTimeFinishedOutOfStdoutFile(self, job_stdout):
		"""
		2008-11-02
			from the "End PBS Epilogue"
			
			...
			----------------------------------------
			Begin PBS Epilogue Tue Oct 28 02:23:07 PDT 2008
			Job ID:           4669781.hpc-pbs.usc.edu
			Username:         yuhuang
			Group:            math-ar
			Job Name:         mpiTopSNPTest_58
			Session:          6586
			Limits:           neednodes=30:myri:ppn=2,nodes=30:myri:ppn=2,walltime=200:00:00
			Resources:        cput=224:41:39,mem=40114912kb,vmem=50415656kb,walltime=03:44:43
			Queue:            cmb
			End PBS Epilogue Tue Oct 28 02:23:13 PDT 2008
			----------------------------------------

		"""
		job_stdout = StringIO.StringIO(job_stdout)
		time_finished = None
		tag = 'End PBS Epilogue'
		for line in job_stdout:
			tag_start_index = line.find(tag)
			if tag_start_index!=-1:
				
				time_finished = datetime.strptime(line[tag_start_index + len(tag) + 1:].strip(), '%a %b %d %H:%M:%S %Z %Y')	#%Z is extra compared to 'qstat -f'
				break
		return time_finished
	
	def updateJobsInDB(self, job_id_set, username):
		"""
		2008-11-04
			a function to qstat all jobs at once, not yet finished
		"""
		commandline = 'qstat -f '
		for job_id in job_id_set:
			commandline += '%s'%(str(job_id))
		
		command_out = self.runRemoteCommand(commandline)
		qstat_for_one_job = ''
		for line in command_out.output_stdout:
			if line.find('Job Id:')==0:
				
				qstat_for_one_job = line
			else:
				qstat_for_one_job += line
			
		if qstat_for_one_job:
			pass
		
	def updateDB(self, username=None, update_node_info=False, jobs_since=None, only_running=False):
		"""
		2008-11-04
			add only_running
			fix jobs_since
		2008-11-01
			0. check in db that are still marked as 'R'
			1. run qstat -u username
			2. pbsnodes
			3. update db
		"""
		sys.stderr.write("%s: Checking the status of the queue ... \n"%datetime.now())
		
		query = JobDB.Job.query
		if only_running:
			query = query.filter_by(state_id='R')
		
		if username:
			query = query.filter_by(username=username)
		if jobs_since:
			query = query.filter(JobDB.Job.time_submitted>=jobs_since)
		
		job_id_ls = [row.id for row in query]
		
		job_id_in_queue = self.get_job_id_ls_from_queue(username)
		
		job_id_set = Set(job_id_ls + job_id_in_queue)
		if update_node_info:
			job_id2current_nodelog_id_ls = self.updateNodeInDB(job_id_set)
		else:
			job_id2current_nodelog_id_ls = None
		for job_id in job_id_set:
			self.updateOneJobInDB(job_id, username)
		sys.stderr.write("Done.\n")
		return job_id_set
		
	
	def fetch_job_stdouterr(self, job_id):
		"""
		2008-11-04
			fix a bug when job_stdouterr from job.job_stdout is None
		2008-11-01
			if it's completed and stdouterr in db, fetch it from db.
			if it's marked as running, fetch it from file and put into db
			
		"""
		job = JobDB.Job.get(job_id)
		job_stdouterr = job.job_stdout
		if job.job_stderr_fname:
			if job_stdouterr:
				job_stdouterr += job.job_stderr
			else:
				job_stdouterr = job.job_stderr
		return job_stdouterr
	
	def fetch_file_content(self, fname):
		"""
		2008-11-01
			fetch file content from remote host
		"""
		commandline = 'cat "%s"'%(fname)
		command_out = self.runRemoteCommand(commandline, report_stderr=False)
		return command_out.output_stdout.read()
	
	def runRemoteCommand(self, commandline, report_stderr=True, report_stdout=False):
		"""
		2008-11-01
			wrapper to run a commandline on the cluster.
		"""
		commandline = 'ssh %s@%s %s'%(self.cluster_username, self.cluster_main_node, commandline)
		return self.runLocalCommand(commandline, report_stderr, report_stdout)
		
	def runLocalCommand(self, commandline, report_stderr=True, report_stdout=False):
		"""
		2008-11-04
			refactor out of runRemoteCommand()
			
			run a command local (not on the cluster)
		"""
		command_handler = subprocess.Popen(commandline, shell=True, \
										stderr=subprocess.PIPE, stdout=subprocess.PIPE)
		output_stdout = command_handler.stdout
		output_stderr = command_handler.stderr
		stderr_content = None
		stdout_content = None
		if report_stderr:
			stderr_content = output_stderr.read()
			if stderr_content:
				sys.stderr.write('stderr of %s: %s \n'%(commandline, stderr_content))
		if report_stdout:
			stdout_content = output_stdout.read()
			if stdout_content:
				sys.stderr.write('stdout of %s: %s \n'%(commandline, stdout_content))
		return_data = PassingData(commandline=commandline, output_stdout=output_stdout, output_stderr=output_stderr,\
								stderr_content=stderr_content, stdout_content=stdout_content)
		return return_data
	
	display_job_var_name_ls = ['id', 'short_name', 'username', 'state_id', 'queue_id', 'no_of_nodes',\
							'ppn', 'memory', 'walltime', 'see_output_while_running', 'time_submitted', 'time_started', 'time_finished']
	
	display_job_log_var_name_ls = ['cput', 'mem_used', 'vmem_used','walltime_used']
	
	var_name2label_type = {'id':('job_id', int),
						'short_name':('job_name', str),
						'state_id':('state', str),
						'queue_id':('queue', str),
						'no_of_nodes':('#nodes', int),
						'ppn': ('ppn', int),
						'see_output_while_running': ('runtime_output', bool)}
	
	def construct_job_label_and_type_ls(self):
		self._display_job_label_ls = []
		self._display_job_label_type_ls = []
		for var_name in self.display_job_var_name_ls + self.display_job_log_var_name_ls:
			if var_name in self.var_name2label_type:
				label, type = self.var_name2label_type[var_name]
			else:
				label = var_name
				type = str
			self._display_job_label_ls.append(label)
			self._display_job_label_type_ls.append(type)
	
	def display_job_label_type_ls(self):
		if getattr(self, '_display_job_label_type_ls', None) is None:
			self.construct_job_label_and_type_ls()
		return self._display_job_label_type_ls
	display_job_label_type_ls = property(display_job_label_type_ls)
	
	def display_job_label_ls(self):
		if getattr(self, '_display_job_label_ls', None) is None:
			self.construct_job_label_and_type_ls()
		return self._display_job_label_ls
	
	display_job_label_ls = property(display_job_label_ls)
	
	def refresh(self, username=None, update_node_info=False, jobs_since=None, only_running=False):
		"""
		2008-11-04
			add job_log info into list_2d only if there is logs from database.
		2008-11-02
			return a 2d list of job information
		"""
		job_id_set = self.updateDB(username, update_node_info, jobs_since, only_running)
		job_id_ls = list(job_id_set)
		job_id_ls.sort()
		list_2d = []
		for job_id in job_id_ls:
			job = JobDB.Job.get(job_id)
			if job:
				row = []
				for var_name in self.display_job_var_name_ls:
					row.append(getattr(job, var_name, None))
				if job.job_log_ls:	#make sure it has logs
					job_log = job.job_log_ls[-1]
				else:
					job_log = None
				for var_name in self.display_job_log_var_name_ls:
					row.append(getattr(job_log, var_name, None))
				list_2d.append(row)
		return list_2d

if __name__ == '__main__':
	from pymodule import ProcessOptions
	main_class = hpc_cmb_pbs
	po = ProcessOptions(sys.argv, main_class.option_default_dict, error_doc=main_class.__doc__)
	instance = main_class(**po.long_option2value)
	if instance.debug:
		import pdb
		pdb.set_trace()
	instance.updateDB(instance.cluster_username)