#!/usr/bin/env python
"""
Usage: QueueInfo.py [OPTIONS]

Option:
	-b, --debug	debug version.
	-r, --report	enable report flag
	-h, --help	Display the usage infomation.

Examples:
	

Description:
	Program to have an overview of the occupation of queues.
		Yu Huang 2006-03-31
"""
import sys, os, getopt, re

def sum(ls):
	s = 0
	for i in ls:
		s += i
	return s


class QueueInfo:
	def __init__(self, debug=0, report=0):
		"""
		01-07-06
		"""
		self.debug = int(debug)
		self.report = int(report)
		self.p_key = re.compile(r'     (?P<key>.*?) = ')
			#12-20-05	? is used to do non-greedy match, for i.e.: '!Platform_description = Keywords = high density oligonucleotide array'
		self.p_value = re.compile(r' = (?P<value>.*)$')
	
	def get_node_arch(self, status_value):
		status_list = status_value.split(',')
		for status_item in status_list:
			status_item_key, status_item_value = status_item.split('=')
			if status_item_key=='arch':
				arch = status_item_value
				return arch
		return None
	
	def parse_one_node_info(self, block, queue_np2job_counter_ls):
		"""
		01-07-06
			(queue_name, np) is key, [0,0,0,0,0] is value, each corresponding to #nodes with 
			a certain number(index) of jobs
		2007-01-20
			deal with old parsing errors for offline nodes
		"""
		job_list = []	#
		arch = None
		np = None
		queue_list = None
		machine_id = block[0].strip()
		for line in block:
			p_key_result = self.p_key.search(line)
			p_value_result = self.p_value.search(line)
			if p_key_result and p_value_result:
				key = p_key_result.group('key')
				value = p_value_result.group('value')
				if key == 'np':
					np = int(value)
				elif key == 'properties':
					queue_list = value.split(',')
				elif key == 'jobs':
					job_list = value.split()
				elif key == 'status':
					arch = self.get_node_arch(value)
		try:
			if arch and np and queue_list:
				for queue in queue_list:
					key = (queue, arch, np)
					if key not in queue_np2job_counter_ls:
						queue_np2job_counter_ls[key] = [0]*5
					old_length = len(queue_np2job_counter_ls[key])-1	#2007-01-20
					if len(job_list)>old_length:
						queue_np2job_counter_ls[key] += [0]*(len(job_list)-old_length)
					queue_np2job_counter_ls[key][len(job_list)] += 1
				return None
			else:
				return machine_id
		except:
			print job_list
	
	def get_free_cpus_from_job_counter_ls(self, np, job_counter_ls):
		no_of_free_cpus = 0
		for i in range(len(job_counter_ls)):
			no_of_free_cpus += (np-i)*job_counter_ls[i]
		return no_of_free_cpus
	
	def run(self):
		inf = os.popen('pbsnodes -a')
		queue_np2job_counter_ls = {}
		block = []
		bad_machine_id_ls = []
		for line in inf:
			if line == '\n':
				bad_machine_id = self.parse_one_node_info(block, queue_np2job_counter_ls)
				if bad_machine_id:
					bad_machine_id_ls.append(bad_machine_id)
				block = []
				continue
			else:
				block.append(line)
		queue_np_job_counter_ls = []
		for queue_np, job_counter_ls in queue_np2job_counter_ls.iteritems():
			queue_np_job_counter_ls.append([queue_np[0], queue_np[1], queue_np[2], \
				sum(job_counter_ls), self.get_free_cpus_from_job_counter_ls(queue_np[2], job_counter_ls), job_counter_ls])
		queue_np_job_counter_ls.sort()
		
		sys.stdout.write('\n')
		header = ['q_name', 'arch', 'np', '#nodes', '#free cpus', 'job_counter_ls']
		sys.stdout.write('%s\n'%'\t'.join(header))
		for row in queue_np_job_counter_ls:
			sys.stdout.write('%s\t%s\t%s\t%s\t%s\t%s\n'%(row[0], row[1], row[2], row[3], row[4], row[5]))
		sys.stdout.write('\n')
		sys.stdout.write("Each number in last column's list is #nodes with 0,1,2,3,4 cpus occupied respectively.\n")
		print
		print "%s bad/offline machines:"%(len(bad_machine_id_ls)), bad_machine_id_ls

if __name__ == '__main__':
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hbr", ["help", "debug", "report"])
	except:
		print __doc__
		sys.exit(2)
	
	debug = 0
	report = 0
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			print __doc__
			sys.exit(2)
		elif opt in ("-b", "--debug"):
			debug = 1
		elif opt in ("-r", "--report"):
			report = 1
	instance = QueueInfo(debug, report)
	instance.run()
