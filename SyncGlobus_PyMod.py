'''
SyncGlobus_PyMod.py

This module contains all the function definitions to sync all dat folders from source to destination. 

For each dat dataset folder, it will:
	1) Sync .dat files over and over until no files are left to sync AND a flag is set that incoming data has finished appearing
	2) Write a flag to the dat folder indicating sync is done
	3) Sync the .log and outgoing_transfer_done_flag_name over to destination with --remove-source-files to clean up folder
	4) Remove any remaninig hidden files (.*) and delete the folder (rmdir)

20140909 CHF - Created
20140915 CHF - Finished!
20140916 CHF - Added step that deletes .dat files after transfer is complete and before flag is written.
20141010 TB  - Implemented the fact that the globust root directory is not necessarily the same
				as the file system directory. The cleanup function was re-written since the "rm %s/*.dat"
				call resulted in a "Argument list too long" error. The rsync was commented out since it's
				no longer needed.
20141011 TB  - Implemented the deletion of files in sets with delete_dat_files flag set prior to transfer.
20141103 TB  - The code now attempts to make sure that the final rsync has started.
20150410 TB  - I updated the dataset deletion logic to also check for no_dp and no_event_build flags before deletion.
'''

import os
import subprocess
import datetime
import time
import re
import sys
from glob import glob
from GlobusTransferTools_PyMod import SubmitGlobusTransfer,GlobusTransferStatus,RunGlobusConnect

class GlobusDaemon:
	'''
	myDaemon = GlobusDaemon(...inputs here...)

	Use myDaemon.start_daemon() to begin infinite sync loop

	'''

	def __init__(self,globus_source,globus_destination,globus_local_command,
		source_data_dir,destination_user,destination_address,destination_data_dir,
		incoming_data_complete_flag_name,outgoing_transfer_done_flag_name,globus_source_path_root = '/',
		globus_destination_path_root = '/', delete_dat_files_flag_name='delete_dat_files', 
		no_dp_flag_name='no_dp', no_event_build_flag_name='no_event_build', 
		execute_delete_dat_files=False):

		# Clean up the input
		if source_data_dir[0] == '~':
			self.source_data_dir = os.path.expanduser(source_data_dir)
		else:
			self.source_data_dir = source_data_dir
		# self.source_data_dir is the path relative to the root directory specified
		# in the globust endpoint. If it is '/' then this is the same as the full
		# path. But, for example, file /Volumes/data/foo.dat would have
		# self.source_data_dir equal to /data/foo.dat if the Globus endpoint 
		# specifies /Volumes/ as its root directory. For cleanup purposes we also
		# may need the full path given in self.source_data_dir_raw
		self.source_data_dir_raw = os.path.join(globus_source_path_root,self.source_data_dir)
		self.globus_source = globus_source
		self.globus_destination = globus_destination
		self.globus_local_command = globus_local_command
		self.destination_user = destination_user
		self.destination_address = destination_address
		self.destination_data_dir = destination_data_dir
		# same as for the source, we need the full path
		self.destination_data_dir_raw = os.path.join(globus_destination_path_root,self.destination_data_dir)
		self.incoming_data_complete_flag_name = incoming_data_complete_flag_name
		self.outgoing_transfer_done_flag_name = outgoing_transfer_done_flag_name
		self.delete_dat_files_flag_name = delete_dat_files_flag_name
		self.no_dp_flag_name = no_dp_flag_name
		self.no_event_build_flag_name = no_event_build_flag_name
		# Find out if the user wants to delete the dat files if the flag is set
		self.execute_delete_dat_files = execute_delete_dat_files

		self.source = '%s/%s/' % (globus_source,source_data_dir)
		self.destination = '%s/%s/' % (globus_destination,destination_data_dir)

		# Constants
		self.sleep_time_sec = 2*60 # 2 mins
		self.rsync_timeout = 90
		self.wait_for_delete_dat_files_flag = 10

	def start_daemon(self):
		print '*** INITIALIZING SYNC ***'
		print '-------------------------'
		print '%s' % time.ctime()
		print '-------------------------'

		# Loop forever
		while True:
			self.SyncFolders()
			# Chill for a bit when there are no data sets to transfer
			time.sleep(self.sleep_time_sec)

	def SyncFolders(self):

		# Get list of all dat folders
		dataset_list_temp = os.listdir(self.source_data_dir_raw)

		# Clean up dat folders.
		dataset_list = [x for x in dataset_list_temp if x[:5]=='lux10' and os.path.isdir(self.source_data_dir_raw+'/'+x)]

		# If there are no datasets in existence...
		if not dataset_list:
			print '%s: Currently nothing to do...' % time.ctime()
			return

		# Make sure than any and all delete_dat_files flags have time to be written/transfered
		time.sleep(self.wait_for_delete_dat_files_flag)
		
		# Loop for every datasets
		for d in dataset_list:

			print '\n\nLooking at dataset %s' % d
			sys.stdout.flush()

			# Build source and destination paths
			self.source_dataset_fullpath = '%s/%s' % (self.source_data_dir,d)
			# specify the full path for cleanup
			self.source_dataset_fullpath_raw = '%s/%s' % (self.source_data_dir_raw,d)

			# Check if acquisition is done
			incoming_transfer_done_flag = self.CheckIncomingTransferDone(d)
			#
			#----------------------------- DAT set deletion
			# The function will go to the next dataset if the current one is 
			# set for deletion or was deleted
			if self.RunDatDeletion(d, incoming_transfer_done_flag): continue
			#-----------------------------
			#
			# We want to wait until all files have been transferred.
			# Otherwise DO NOTHING for this dataset
			# If it's done getting here, then...
			if incoming_transfer_done_flag:

				# Check if this job has already been submitted.
				# An empty transfer_id means it has not been submitted.
				transfer_id = self.GetGlobusTaskID(d)

				# If this dataset has been submitted to Globus
				if transfer_id:

					print "%s: Dataset %s, transfer task found: %s" % (time.ctime(),d,transfer_id)

					# Get the transfer details for this ID
					globus_transfer_details = GlobusTransferStatus(transfer_id)

					self.PrintTransferDetails(globus_transfer_details)

					# If it's done, write flag and clean up
					if (globus_transfer_details['status'] == 'COMPLETE') or (globus_transfer_details['status'] == 'SUCCEEDED'):

						# Once the previous while loop is happy, finish it off by:
						# 	(1) Write the rsync_done_flag
						#	(2) rsync --remove-source-files   (this will sync .log and rsync_done_flag for the first time)
						print '%s: Transfer %s done, writing flag, syncing log and deleting sources!' % (time.ctime(),d)

						# Write sync done flag
						os.system('touch %s/%s' % (self.source_dataset_fullpath_raw,self.outgoing_transfer_done_flag_name))

						# rsync with --remove-source-files and delete folder
						self.CleanUpAndDelete(d)

					# If it's still ongoing, just print status info
					elif globus_transfer_details['status'] == 'ACTIVE':
						print 'Transfer task currently active'

					# If it failed, resubmit!
					elif globus_transfer_details['status'] == 'FAILED':
						print '*** Transfer failed. Details:'

						print 'Resubmitting transfer with the same ID...'
						# Re-submit!
						transfer_id, transfer_label = SubmitGlobusTransfer(self.source, self.destination, d, 'lux', transfer_id)
						print transfer_id
						if transfer_id and (transfer_id != -1):
							print 'Resubmitted successfully!'
						else:
							print '*** ERROR: Could not submit job. There may be a problem with one of the endpoints (check that Globus is running and credentials have not expired).'
							# Try to activate the endpoint
							RunGlobusConnect(self.globus_source,self.globus_local_command,'lux')

					else:
						print 'Not sure what to do here... (unknown status)'

				# If it has not been submitted to Globus, do so!
				else:

					# Submit to Globus
					transfer_id, transfer_label = SubmitGlobusTransfer(self.source, self.destination, d, 'lux')

					if transfer_id and (transfer_id != -1):
						print '%s: Submitted dataset %s with transfer ID %s' % (time.ctime(),d,transfer_id)
						# Write the transfer_id flag in the directory
						os.system('touch %s/globus_transfer_%s' % (self.source_dataset_fullpath_raw,transfer_id))
					else:
						print '*** ERROR: Could not submit job. There may be a problem with one of the endpoints (check that Globus is running and credentials have not expired).'

						# Try to activate the endpoint
						RunGlobusConnect(self.globus_source,self.globus_local_command,'lux')
						

			# It's not done syncing, leave it alone for now
			else:
				print "Skipping %s for now, it's not done syncing to %s (could not find %s)\nTransfer will begin when all files are in %s..." % (d,self.globus_source,self.incoming_data_complete_flag_name, self.globus_source)

		# Once all folders have been inspected/acted on, sleep for a bit
		print 'Sleeping for %d seconds' % self.sleep_time_sec
		time.sleep(self.sleep_time_sec)			

	def GetGlobusTaskID(self,dataset):
		# Initialize
		transfer_id = None

		# See if any globus task ID file is in the folder
		globus_task_id_file_wildcard = '%s/%s/globus_transfer_*' % (self.source_data_dir_raw,dataset)
		task_id_file = glob(globus_task_id_file_wildcard)

		# If you found one, get the task ID and return it
		if task_id_file:
			transfer_id = re.findall('\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$',task_id_file[0])[0]

		return transfer_id

	def CheckIncomingTransferDone(self,dataset):

		# Re-doing here so that the function can be called without running the main loop
		source_dataset_fullpath_raw = '%s/%s/' % (self.source_data_dir_raw,dataset)

		# Get flag status tuple
		# Using glob here instead of os.path.isfile since glob can use * wildcards, which are used for daquiri transfers
		incoming_data_complete_flag = glob(source_dataset_fullpath_raw + "/" + self.incoming_data_complete_flag_name)

		# Check if incoming data is finished syncing
		if incoming_data_complete_flag:
			return 1
		else:
			return 0

	def CheckDeleteDoneFlag(self,dataset):
		"""
		Checks if the delete_dat_files is present. Also checks the no_event_build and 
		no_dp flags to make sure the deletion can happen on LOC.
		"""

		# Re-doing here so that the function can be called without running the main loop
		source_dataset_fullpath_raw = '%s/%s/' % (self.source_data_dir_raw,dataset)

		# Get flag status tuple
		# Using glob here instead of os.path.isfile since glob can use * wildcards, which are used for daquiri transfers
		delete_dat_files_flag = glob(source_dataset_fullpath_raw + "/" + self.delete_dat_files_flag_name)
		no_dp_flag = glob(source_dataset_fullpath_raw + "/" + self.no_dp_flag_name)
		no_event_build_flag = glob(source_dataset_fullpath_raw + "/" + self.no_event_build_flag_name)
		
		# Check if delete_dat_files flag is present
		if delete_dat_files_flag and no_dp_flag and no_event_build_flag:
			return 1
		else:
			return 0

	def RunDatDeletion(self, d, incoming_transfer_done_flag):
		"""
		Handle the deletion of the data set. Either make sure that the delete_dat_files
		flag gets transfered first or that the set is deleted here depending on the
		settings. 
		"""
		# Check if the flag marking that the set is to be deleted is set
		delete_dat_files_flag = self.CheckDeleteDoneFlag(d)
		if delete_dat_files_flag:
			print 'DAT files in set %s are marked for deletion' %(d)
			# check if the dat files are to be deleted here
			if self.execute_delete_dat_files:
				print 'DAT files in set %s will be deleted when acquisition/transfer finishes' %(d)
				# Delete files after they've been transferred here. Do it after
				# because DPM may need them. Do NOT transfer them to the next
				# computer in the chain. Also, go on copying the rest of the
				# data sets while this one finish transfering here. 
				if incoming_transfer_done_flag:
					# Set has nominally finished transfering. But wait a little
					# bit to make sure we get the stragglers
					print 'DAT files in set %s will be deleted in %.1f seconds' %(d, self.sleep_time_sec*2.)
					time.sleep(self.sleep_time_sec*2)
					os.system("rm -r %s" % self.source_dataset_fullpath_raw)
					return True		# success. Go to the next dataset
				else:
					# the set to be deleted hasn't transfered yet.
					#continue	# go to the next data set. 
					return True
			else:
				# files aren't deleted here but on subsequent machine. But globus
				# waits for the full set to be sent which means I don't have to worry
				# about sending the delete flag first
				pass
		
		return False

	def CleanUpAndDelete(self,dataset):
		#
		"""# Sync one last time with --remove-source-files
		print 'Cleaning up .dat files...'
		rsync_command_rmfiles = "rm %s/*.dat" % self.source_dataset_fullpath_raw
		p1 = subprocess.Popen(rsync_command_rmfiles, stdout=subprocess.PIPE, shell=True)
		p1.wait()
		"""
		# 20141010 TB: The above doesn't work; you'll get the 
		# "/bin/sh: /bin/rm: Argument list too long"
		# error. Hence below I remove the set recursively.
		#
		# but first sync up any remaining flags, but not dat files
		rsync_command_cleanup = \
			"rsync --remove-source-files -aP --timeout=%s --exclude='*.dat' --exclude='.*' --progress %s %s@%s:%s/"\
				% (self.rsync_timeout, self.source_dataset_fullpath_raw, self.destination_user, \
				self.destination_address, self.destination_data_dir_raw)
		print 'final rsync command:'
		print rsync_command_cleanup
		p = subprocess.Popen(rsync_command_cleanup, stdout=subprocess.PIPE, shell=True)
		rsync_read_loop_counter = 0
		rsync_failure_counter = 0
		while True:
			line = p.stdout.readline()
			if line: 
				print '.',
				rsync_read_loop_counter += 1
			else:
				if rsync_read_loop_counter > 0 or rsync_failure_counter > 1:
					print ' '
					if rsync_failure_counter > 1:
						print 'We had an issue with the rsync. The sync flag may not have been transfered.'
					break
				else:
					# We get here if rsync start was delayed. Then the first
					# readline() returns nothing and the code skipps to the 
					# here. In the past, the directory would be deleted and 
					# the sync flag would not be transfered. We deal with this
					# here. 
					# wait here instead
					print 'rsync did not seem to be initiated in time. Wait a bit.'
					rsync_failure_counter += 1
					time.sleep(self.rsync_timeout)
		# remove the transfered directory
		print 'recursively delete the dataset'
		command_rm = "rm -r %s" % self.source_dataset_fullpath_raw
		p1 = subprocess.Popen(command_rm, stdout=subprocess.PIPE, shell=True)
		p1.wait()
		# The below is now useless since the dataset was completally removed. 
		"""
		# Delete any remaining hidden files
		os.system("find %s -name '.*' | xargs -n1 rm" % self.source_dataset_fullpath_raw)

		# Now delete the empty folder, we're done here
		os.system('rmdir %s' % self.source_dataset_fullpath_raw)
		"""
		print '*'*40 + '\nFinished!\n' + '*'*40

	def PrintTransferDetails(self,globus_transfer_details):
		print '%s' % '-'*36
		print '%s' % globus_transfer_details['transfer_id']
		print '%s' % '-'*36

		for key,val in globus_transfer_details.iteritems():
			if key != 'transfer_id':
				print '%20s: %s' % (self.FormatName(key),str(val))

	def FormatName(self,str):
		return str.replace('_',' ').title()

