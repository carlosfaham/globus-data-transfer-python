'''
GlobusTransferTools_PyMod.py

20140910 CHF - Created
20141010 TB  - Made the transfer_label be constructed instead of hard-coded.
				Fixed a bug where the script attempted to gnerate an ID event
				though it obtained an existing one.
20141010 TB  - GlobusActivateEndpoint had a bug where globus_endpoint wasn't
				passed to the command and the command had a copy/past error
20141125 CHF - Added source_endpoint, destination_endpoint, label to dictionary output
				in GlobusTransferStatus
20141126 CHF - Minor bug fix in globus_transfer_details['source_endpoint'] (character was being deleted)
'''

from subprocess import Popen, PIPE, STDOUT
import os

def SubmitGlobusTransfer(source,destination,dataset,user,transfer_id=''):

	# If no transfer ID specified, get a new one
	if not transfer_id:
		# Command to generate the unique transfer ID
		genid_command = 'ssh %s@cli.globusonline.org transfer --generate-id' % user
		# Execute command
		p = Popen(genid_command, stdout=PIPE, shell=True)
		# Grab transfer ID
		transfer_id = p.communicate()[0].strip() # something that looks like 98d0879c-3919-11e4-b5ed-12313940394d

	# Give this transfer a more legible, yet unique label
	from_location = source.split(os.path.sep)[0].split('#')[1]
	to_location = destination.split(os.path.sep)[0].split('#')[1]
	transfer_label = '%s_%s_%s_%s' % (dataset,from_location,to_location,transfer_id[:8])

	# The Globus transfer command
	transfer_command = '''echo "%s/%s/ %s/%s/ -r" | ssh %s@cli.globusonline.org transfer --taskid='%s' -s 3 --label='%s' ''' % (source,dataset,destination,dataset,user,transfer_id,transfer_label)

	p2 = Popen(transfer_command, stdout=PIPE, shell=True)
	return_val = p2.communicate()
	print return_val

	if return_val[0].find(transfer_id) != -1:
		return transfer_id, transfer_label
	else:
		transfer_id = -1
		transfer_label = -1
		return transfer_id, transfer_label

def GlobusTransferStatus(transfer_id,user='lux'):

	# Get details for task ID
	globus_details_command = "ssh -Y %s@cli.globusonline.org details %s" % (user,transfer_id)
	p = Popen(globus_details_command, stdout=PIPE, shell=True)
	(globus_details_raw,err) = p.communicate()

	# Parse output
	details_raw = globus_details_raw.split('\n\n')
	details_split = details_raw[0].split('\n')

	# Convert output into a dictionary
	globus_transfer_details = dict()
	globus_transfer_details['transfer_id'] = transfer_id
	globus_transfer_details['task_type'] = details_split[1].split(': ')[1]
	globus_transfer_details['status'] = details_split[2].split(': ')[1]
	globus_transfer_details['completion_time'] = details_split[5].split(': ')[1]
	globus_transfer_details['tasks_total'] = details_split[6].split(': ')[1]
	globus_transfer_details['tasks_successful'] = details_split[7].split(': ')[1]
	globus_transfer_details['tasks_failed'] = details_split[10].split(': ')[1]
	globus_transfer_details['tasks_pending'] = details_split[11].split(': ')[1]
	globus_transfer_details['transfer_name'] = details_split[14].split(': ')[1]
	globus_transfer_details['files'] = details_split[21].split(': ')[1]
	globus_transfer_details['files_skipped'] = details_split[22].split(': ')[1]
	globus_transfer_details['directories'] = details_split[23].split(': ')[1]
	globus_transfer_details['bytes_transferred'] = details_split[25].split(': ')[1]
	globus_transfer_details['faults'] = details_split[28].split(': ')[1]
	globus_transfer_details['source_endpoint'] = details_split[15].split(': ')[1].strip('%s' % user).strip('#')
	globus_transfer_details['destination_endpoint'] = details_split[16].split(': ')[1].strip('%s#' % user)
	globus_transfer_details['label'] = details_split[14].split(': ')[1]

	return globus_transfer_details

def GlobusActivateEndpoint(globus_endpoint,user='lux'):
	activation_command = 'ssh %s@cli.globusonline.org endpoint-activate %s' % (user,globus_endpoint)

	p = Popen(activation_command, stdout=PIPE, shell=True)
	(activation_details_raw,err) = p.communicate()

	print activation_details_raw

def RunGlobusConnect(globus_endpoint,local_command,user='lux'):

	status_check_command = 'ssh %s@cli.globusonline.org endpoint-list -v -f credential_status %s' % (user,globus_endpoint)

	p = Popen(status_check_command, stdout=PIPE, shell=True)
	(endpoint_details_raw,err) = p.communicate()

	# Parse output
	endpoint_status = endpoint_details_raw.strip().split(': ')[1]

	print 'Status for %s is %s' % (globus_endpoint,endpoint_status)

	if endpoint_status == 'ACTIVE':
		print 'Globus Connect currently running for %s. Nothing to do.' % globus_endpoint
	else:
		print 'Executing local command to re-animate Globus at %s' % globus_endpoint
		p = Popen(local_command, stdout=PIPE, shell=True)
		(out,err) = p.communicate()

		print 'Activating endpoint %s' % globus_endpoint
		GlobusActivateEndpoint(globus_endpoint,'lux')

def FormatCLIOutputDict(globus_details_raw):

	output_dict = dict()

	details_raw = globus_details_raw.split('\n\n')
	details_split = details_raw[0].split('\n')

	for line in details_split:

		a = line.split[': '][0]
		b = line.split[': '][0]
		field_name = a.lower().replace(' ')





