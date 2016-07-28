#!/usr/bin/env python
#
# by lalves 2016
# 
# Usage: Replace FQDN1 with your slurm enabled host FQDN and CLUSTERNAME1 with correspondent cluster name in Slurm DB.
# 	 Add as many hosts as necessary.
#	 Key based SSH authentication in place is assumed here.


import datetime
import subprocess

starttime = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y-%m-%dT%H:%M:%S')
endtime = datetime.datetime.now().strftime('%Y-%m-%dT00:00:00')

clusters = {
	'FQDN1': 'CLUSTERNAME1',
	'FQDN2': 'CLUSTERNAME2',
	'FQDN3': 'CLUSTERNAME3',
	'FQDN4': 'CLUSTERNAME4',
	'FQDN5': 'CLUSTERNAME5'
}

clustersItems = clusters.items()

for c in clustersItems:
	subprocess.call('ssh -q -i ~/.ssh/id_rsa {} /usr/bin/env TZ=UTC sacct --clusters {} --allusers --parsable2 --noheader --allocations --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime {} --endtime {} >/tmp/slurm-{}.log'.format(c[0],c[1],starttime,endtime,c[1]), shell=True)
	subprocess.call('CMDxdmod-shredder -f slurm -r {} -i /tmp/slurm-{}.log'.format(c[1],c[1]), shell=True)
subprocess.call('xdmod-ingestor -v', shell=True)

