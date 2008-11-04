#!/usr/bin/env python
"""
2008-11-01
	one backend for grid_job_mgr.py to handle the cmb internal cluster (Drake used to maintain it).
	it's designed to utilize both a bunch of debian nodes that are out of pbs control and the pbs queue.
	
	it's defunct at this moment.
	
	split from grid_job_mgr.py
"""
import sys, os, math
#bit_number = math.log(sys.maxint)/math.log(2)
#if bit_number>40:       #64bit
sys.path.insert(0, os.path.expanduser('~/lib/python'))
sys.path.insert(0, os.path.join(os.path.expanduser('~/script/')))


class node_process_data:
	"""
	03-20-05
		store the process information
	"""
	def __init__(self):
		self.label_list = []
		self.ps_output_list = []
		self.pid2index = {}
	
class cmb_cluster(object):
	"""
	2008-11-02
		renamed from node_process to cmb_cluster
		currently not functional anymore
	03-20-05
		get process information given a node_range
		kill a specific process
	"""
	def __init__(self):
		self.re_pid = re.compile(r'^\ *(\d+)\ ')	#to catch the pid(first group)
	
	def get_processes(self, node_range, ps_command, username, outputfile):
		"""
		10-23-05
			don't insert 'node' before item in node_range
		"""
		sys.stderr.write("Getting process information for %s...\n"%(username))
		node2ps = {}
		for node in node_range:
			print "%s"%node
			#first execute
			ps_option = '-o pid,ppid,pcpu,pmem,stime,stat,time -U %s'%username
			command = 'ssh %s %s %s > %s'%(node, ps_command, ps_option, outputfile)
			wl = ['sh', '-c', command]
			return_code = os.spawnvp(os.P_WAIT, 'sh', wl)
			if return_code!=0:	#normal exit code for ssh is 0
				sys.stderr.write("%s sh -c ssh error code:%s, ignore.\n"%(node, return_code))
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
			command = 'ssh %s %s %s > %s'%(node, ps_command, ps_option, outputfile)
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
		09-16-05
			use the exit_code to judge if successful or not
		10-23-05
			remove 'node' before the number
		"""
		command = 'ssh %s kill  -15 %s'%(node_number, pid)
		exit_code = os.system(command)
		if exit_code==0:
			print "process %s on node %s killed"%(pid, node_number)
		else:
			print "Failed to kill process %s on node %s"%(pid, node_number)
		"""
		wl = ['sh', '-c', command]
		os.spawnvp(os.P_WAIT, 'sh', wl)
		print "process %s on node %s killed"%(pid, node_number)
		"""
	
	def submit_jobs(self, job_list, job_fprefix, job_starting_number, \
		no_of_nodes, qsub_option, ppn=None, time_to_run=None, node_range=None, submit_option=None):
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
		06-24-05
			if user specified time_to_run, then schedule it to qsub
		10-23-05
			'node' before number is removed.
		"""
		if not time_to_run:
			time_tuple = time.localtime()
			time_to_run_jobs = "%s:%s"%(time_tuple[3], time_tuple[4]+2)
		else:
			time_to_run_jobs = time_to_run

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
						jobrow = 'ssh %s %s'%(node, sub_job)
					job_f.write('echo %s\n'%jobrow)	#print the commandline
					job_f.write("%s\n"%jobrow)	#command here
				job_f.write('date\n')	#the ending time
				#close the file
				job_f.close()
				
				print "node: %s, at %s, job: %s"%(node, time_to_run_jobs, job)
				if submit_option == 1:
					if time_to_run:
						jobrow = "echo qsub -@ ~/.qsub.options %s | at -m %s"%(job_fname, time_to_run)	#06-24-05 if user specified time_to_run, then schedule it to qsub
					else:
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
		