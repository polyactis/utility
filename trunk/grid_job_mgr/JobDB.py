#!/usr/bin/env python
"""
Examples:
	#setup database in postgresql
	JobDB.py -v postgres -u crocea -z localhost -d graphdb -k public
	
	#setup database in mysql
	JobDB.py -v mysql -u yh -z localhost -d cluster_job -k ""
	
Description:
	2008-11-02
		database to hold data related to cluster jobs (hpc-cmb)
"""
import sys, os, math
bit_number = math.log(sys.maxint)/math.log(2)
#if bit_number>40:       #64bit
#	sys.path.insert(0, os.path.expanduser('~/lib64/python'))
#	sys.path.insert(0, os.path.join(os.path.expanduser('~/script64')))
#else:   #32bit
sys.path.insert(0, os.path.expanduser('~/lib/python'))
sys.path.insert(0, os.path.join(os.path.expanduser('~/script')))

from sqlalchemy.engine.url import URL
from elixir import Unicode, DateTime, String, Integer, UnicodeText, Text, Boolean, Float, Binary
from elixir import Entity, Field, using_options, using_table_options
from elixir import OneToMany, ManyToOne, ManyToMany
from elixir import setup_all, session, metadata, entities
from elixir.options import using_table_options_handler	#using_table_options() can only work inside Entity-inherited class.
from sqlalchemy import UniqueConstraint, create_engine
from sqlalchemy.schema import ThreadLocalMetaData, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker

from datetime import datetime

from pymodule.db import ElixirDB

__session__ = scoped_session(sessionmaker(autoflush=False, transactional=False))
#__metadata__ = ThreadLocalMetaData()

__metadata__ = MetaData()

class README(Entity):
	#2008-08-07
	title = Field(String(2000))
	description = Field(String(60000))
	created_by = Field(String(128))
	updated_by = Field(String(128))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='readme', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')

class JobState(Entity):
	short_name = Field(String(512), primary_key=True)
	description = Field(Text)
	created_by = Field(String(128))
	updated_by = Field(String(128))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='job_state', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')

class Job(Entity):
	id = Field(Integer, primary_key=True)
	short_name = Field(String(2048))
	how_job_ended = ManyToOne("JobState", colname='how_job_ended_id', ondelete='CASCADE', onupdate='CASCADE')
	job_state = ManyToOne("JobState", colname='state_id', ondelete='CASCADE', onupdate='CASCADE')
	content = Field(Binary(134217728), deferred=True)
	job_fname = Field(Text)
	job_stdout = Field(Binary(134217728), deferred=True)
	job_stderr = Field(Binary(134217728), deferred=True)
	job_stdout_fname = Field(Text)
	job_stderr_fname = Field(Text)
	
	username = Field(String(256))
	
	queue = ManyToOne("NodeProperty", colname='queue_id', ondelete='CASCADE', onupdate='CASCADE')
	server = Field(String(1024))
	no_of_nodes = Field(Integer)
	ppn = Field(Integer)
	memory = Field(String(1024))
	walltime = Field(String(1024))
	workdir = Field(String(1024))
	see_output_while_running = Field(Boolean)
	time_submitted = Field(DateTime, default=datetime.now)
	time_started = Field(DateTime)
	time_finished = Field(DateTime)
	#nodes = ManyToMany('Node', tablename='job2node', ondelete='CASCADE', onupdate='CASCADE')	#local_side, remote_side
	nodes = OneToMany("Job2Node")
	job_log_ls = OneToMany("JobLog")
	created_by = Field(String(200))
	updated_by = Field(String(200))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='job', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')

class JobLog(Entity):
	job = ManyToOne("Job", colname='job_id', ondelete='CASCADE', onupdate='CASCADE')
	cput = Field(String(1024))
	mem_used = Field(String(1024))
	vmem_used = Field(String(1024))
	walltime_used = Field(String(1024))
	 
	created_by = Field(String(200))
	updated_by = Field(String(200))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='job_log', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')


"""
#2008-11-02 try ManyToMany in another way according to http://elixir.ematia.de/trac/wiki/AutoLoadingTips. doesn't work.
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
node2property = Table('node2property', __metadata__,
					Column('node_name',String(512) , ForeignKey('node.short_name'), primary_key=True),
					Column('property_name',String(512) , ForeignKey('node_property.short_name'), primary_key=True),
					)

"""

class Job2Node(Entity):
	node = ManyToOne("%s.Node"%__name__, colname='node_name', ondelete='CASCADE', onupdate='CASCADE', primary_key=True)
	job = ManyToOne("%s.Job"%__name__, colname='job_id', ondelete='CASCADE', onupdate='CASCADE', primary_key=True)
	using_options(tablename='job2node', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')
	

class NodeProperty(Entity):
	#2008-11-02 store queue name and other node categories
	short_name = Field(String(512), primary_key=True)
	nodes = OneToMany("Node2Property")
	#nodes = ManyToMany("Node", tablename='node2property', ondelete='CASCADE', onupdate='CASCADE')	#2008-11-02 doesn't work. complain that it can't find table named 'node'
	#nodes = ManyToMany("Node", tablename='node2property', ondelete='CASCADE', onupdate='CASCADE',
	#				foreign_keys=lambda: [node2property.c.node_name, node2property.c.property_name],
	#		   		primaryjoin=lambda: NodeProperty.short_name == node2property.c.property_name,
    #   			secondaryjoin=lambda: Node.short_name == node2property.c.node_name,)
	created_by = Field(String(200))
	updated_by = Field(String(200))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='node_property', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')

class Node2Property(Entity):
	node = ManyToOne("%s.Node"%__name__, colname='node_name', ondelete='CASCADE', onupdate='CASCADE', primary_key=True)
	property = ManyToOne("%s.NodeProperty"%__name__, colname='property_name', ondelete='CASCADE', onupdate='CASCADE', primary_key=True)
	using_options(tablename='node2property', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')

class Node(Entity):
	short_name = Field(String(512), primary_key=True)
	properties = OneToMany("%s.Node2Property"%__name__)
	#properties = ManyToMany("NodeProperty", tablename='node2property', ondelete='CASCADE', onupdate='CASCADE',
	#					foreign_keys=lambda: [node2property.c.node_name, node2property.c.property_name],
    #    				primaryjoin=lambda: Node.short_name == node2property.c.node_name,
    #    	   			secondaryjoin=lambda: NodeProperty.short_name == node2property.c.property_name,)
	ncpus = Field(Integer)
	arch = Field(String(512))
	opsys = Field(String(1024))
	totmem = Field(Text)
	physmem = Field(Text)
	uname = Field(Text)
	
	#jobs = ManyToMany('Job', tablename='job2node', ondelete='CASCADE', onupdate='CASCADE')
	jobs = OneToMany("%s.Job2Node"%__name__)
	log_ls = OneToMany("%s.NodeLog"%__name__)
	created_by = Field(String(200))
	updated_by = Field(String(200))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='node', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')

class NodeQueue(Entity):
	"""
	2010-3-3
		a queue holding a list of nodes for status checking
	"""
	node = ManyToOne("%s.Node"%__name__, colname='node_name', ondelete='CASCADE', onupdate='CASCADE')
	queue_type = ManyToOne("%s.QueueType"%__name__, colname='queue_type_id', ondelete='CASCADE', onupdate='CASCADE')
	created_by = Field(String(200))
	updated_by = Field(String(200))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='node_queue', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')
	using_table_options(UniqueConstraint('node_name', 'queue_type_id'))

class QueueType(Entity):
	"""
	2010-3-3
		type of queue
	"""
	short_name = Field(String(512), unique=True)
	description = Field(String(8124))
	created_by = Field(String(200))
	updated_by = Field(String(200))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='queue_type', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')

class NodeLog(Entity):
	node = ManyToOne('%s.Node'%__name__, colname='node_id', ondelete='CASCADE', onupdate='CASCADE')
	state = Field(String(1024))
	nsessions = Field(Integer)
	nusers = Field(Integer)
	idletime = Field(Integer)
	availmem = Field(Text)
	loadave = Field(Float)
	netload = Field(Float)
	size = Field(Text)
	rectime = Field(Integer)
	created_by = Field(String(200))
	updated_by = Field(String(200))
	date_created = Field(DateTime, default=datetime.now)
	date_updated = Field(DateTime)
	using_options(tablename='node_log', metadata=__metadata__, session=__session__)
	using_table_options(mysql_engine='InnoDB')


class ClusterJobDB(ElixirDB):
	__doc__ = __doc__
	option_default_dict = ElixirDB.option_default_dict.copy()
	option_default_dict[('drivername', 1,)][0] = 'postgres'
	option_default_dict[('database', 1,)][0] = 'graphdb'
	option_default_dict[('schema', 0,)][0] = 'cluster_job'
	option_default_dict.pop(('password', 1, ))
	option_default_dict.update({('password', 0, ):[None, 'p', 1, 'database password', ]})
	def __init__(self, **keywords):
		"""
		2008-10-29
			database to control cluster jobs
		"""
		from pymodule import ProcessOptions
		ProcessOptions.process_function_arguments(keywords, self.option_default_dict, error_doc=self.__doc__, class_to_have_attr=self)
		self.setup_engine(metadata=__metadata__, session=__session__, entities=entities)

if __name__ == '__main__':
	from pymodule import ProcessOptions
	main_class = ClusterJobDB
	po = ProcessOptions(sys.argv, main_class.option_default_dict, error_doc=main_class.__doc__)
	instance = main_class(**po.long_option2value)
	instance.setup()
	import pdb
	pdb.set_trace()
