#!/usr/bin/python
import sys
import os
import subprocess

# global params
dbg = False
package = 'org.corfudb.sharedlog'

					# get the parent dir of the script, the .jar is relative to it
parentpath,junk = os.path.split(os.path.dirname(os.path.realpath(sys.argv[0])))  
cp = parentpath+'/CORFULIB/target/corfu-lib-0.1-SNAPSHOT-shaded.jar'

# parse command-line arguments:
#   [-rammode] [-recover]
#
rammode, recover = False, False

for arg in sys.argv[1:]:
	if arg == "-recover" :
		print "recovery mode"
		recover = True
	if arg == "-rammode" :
		print "ram mode"
		rammode = True
# #############################

# the configuration is provided in corfu.xml:
#
import xml.etree.ElementTree as etree
tree = etree.parse('corfu.xml')
root = tree.getroot()
if (dbg): print root.tag, root.text
# #############################

# start sequencer 
# #################
seq = root.get('sequencer')
print 'sequencer is on' , seq
host,port = seq.split(':')
smainclass = package+".sequencer.SequencerDriver"
print 'running: java -classpth',cp,smainclass,port
subprocess.Popen(['java', '-cp', cp, smainclass , port])
				# TODO: invoke non-locally!

# start logging units
# ######################
umainclass = package+".loggingunit.LogUnitDriver"
gcount=0
ucount = 0
for unit in root.iter('NODE') :
	if (dbg): print unit.tag, unit.attrib

	host,port = unit.get('nodeaddress').split(':')

	if (dbg): print host
	if (dbg): print port

	print 'running: java -classpth',cp,umainclass,'-port',port,'-rammode'
	subprocess.Popen(['java', 
			'-cp', cp, 
			umainclass , 
			'-port',port, 
			'-rammode'])
					# TODO: invoke non-locally!

	ucount = ucount+1


# start the configuration master
# #############################
import time
time.sleep(1)

mmainclass = package+".ConfigMasterDriver"
print 'running: java -classpth',cp,mmainclass
subprocess.Popen(['java', '-cp', cp, mmainclass ])
