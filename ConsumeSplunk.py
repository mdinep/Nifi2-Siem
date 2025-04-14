from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.componentstate import Scope, StateManager, StateException
import os, sys
import datetime as dt
import time
import subprocess

class ConsumeSplunkProcessor(FlowFileSource):
	class Java:
		implements = ['org.apache.nifi.python.processor.FlowFileSource']

	class ProcessorDetails:
		version = '0.0.1-SNAPSHOT'
		description = '''This processor uses the Splunk search utility to consume the contents of a Splunk bucket and output it as flowfile content for downstream processing.'''
		tags = ["splunk","cribl","bucket","index","siem"]

	def __init__(self, jvm=None, **kwargs):
		self.jvm=jvm
		
		self.SPLUNK_INSTALL_DIR = PropertyDescriptor(
			name="Splunk install path",
			description="""This is the path to the directory where Splunk is installed.""",
			required=True,
		)
        
		self.INDEX_NAME = PropertyDescriptor(
			name="Splunk index name",
			description="""The name of the Splunk index, or bucket, from which records will be consumed""",
			required=True,
		)
        
		self.OUTPUT_FORMAT = PropertyDescriptor(
			name="Data output format",
			description="""This is the format in which the data exported from Splunk will be converted""",
			allowable_values=["raw","csv","json"],
			required=True,
			default_value="csv"
        )
        
		self.MAX_OUTPUT = PropertyDescriptor(
			name="Max records to output",
			description='''This is the maximum number of records to output from a Splunk Bucket, starting with newest. Default value is 0, which will output the entire bucket''',
			required=False
        )
        
		self.MIN_DATE = PropertyDescriptor(
			name="Date Range Start",
			description='''This is the start date for a date range in which records are to be consumed. Supports ISO8601 formatted as yyyy-mm-dd:HH:mm:ss, relative time, and several '''\
			'''application-specific modifiers. See Time Modifiers in the Splunk search module for more information: https://docs.splunk.com/Documentation/SCS/current/Search/Timemodifiers''',
			required=False,
        )
        
		self.MAX_DATE = PropertyDescriptor(
			name="Date Range End",
			description='''This is the start date for a date range in which records are to be consumed. Supports ISO8601 formatted as yyyy-mm-dd:HH:mm:ss, relative time, and several '''\
			'''application-specific modifiers. See Time Modifiers in the Splunk search module for more information: https://docs.splunk.com/Documentation/SCS/current/Search/Timemodifiers''',
			required=False,
		)
        
		self.SPLUNK_USER = PropertyDescriptor(
			name="Splunk Username",
			description="""The username of the account used to access and export data from Splunk""",
			required=False
		)
        
		self.SPLUNK_PASS = PropertyDescriptor(
			name="Splunk User Password",
			description="""The password for the account used to access and export data from Splunk. This is a sensitive property and will be obfuscated""",
			required=False,
			sensitive=True
		)
        
		self.property_descriptors = [self.SPLUNK_INSTALL_DIR, self.INDEX_NAME, self.OUTPUT_FORMAT, self.MAX_OUTPUT, self.MIN_DATE, self.MAX_DATE, self.SPLUNK_USER, self.SPLUNK_PASS]
        
	def getPropertyDescriptors(self):
		return self.property_descriptors
        
	def onScheduled(self, context):
		try:
			self.state = context.getStateManager().getState(Scope.CLUSTER).toMap()
		except StateException as e:
			self.logger.warn('Failed to read processor state. ' + str(e))
			self.state = dict()
		self.splunk_install_dir = context.getProperty(self.SPLUNK_INSTALL_DIR).getValue()
		self.splunk_index_name = context.getProperty(self.INDEX_NAME).getValue()
		self.output_format = context.getProperty(self.OUTPUT_FORMAT).getValue()
		self.max_output = context.getProperty(self.MAX_OUTPUT).getValue()
		self.min_date = context.getProperty(self.MIN_DATE).getValue()
		self.max_date = context.getProperty(self.MAX_DATE).getValue()
		self.splunk_user = context.getProperty(self.SPLUNK_USER).getValue()
		self.splunk_pw = context.getProperty(self.SPLUNK_PASS).getValue()
            
	def buildSearchParams(self):
		paramString = self.splunk_index_name
		if self.min_date != None:
			paramString = "".join((" earliest=",self.min_date))
		if self.max_date != None:
			paramString = "".join((" latest=",self.max_date))
		return paramString
            
	def addOptionalProperties(self):
		optionalProps = ""
		if self.max_output != None:
			optionalProps = " ".join((optionalProps,"-maxout",self.max_output))
		if self.splunk_user != "":
			if self.splunk_pw != None:
				optionalProps = " ".join((optionalProps,"-auth",self.splunk_user))
				optionalProps = "".join((optionalProps, ":", "'", self.splunk_pw, "'"))
			else:
				self.logger.error("Splunk user account provided, but password is missing. Please update account properties.")
		return optionalProps
    	
	def buildAndRunQuery(self):
		try:
			queryString = "".join((self.splunk_install_dir.rstrip("/"),"/bin/splunk search \"index=",self.buildSearchParams(),"\" -output ",self.output_format,self.addOptionalProperties()))
			cmdOutput = subprocess.run([queryString],  shell=True, capture_output=True, encoding='utf-8', text=True)
			if cmdOutput.stderr == "":
				return cmdOutput.stdout
			else:
				self.logger.error("Failed to export data from Splunk: " + cmdOutput.stderr)
				return cmdOutput.stdout
		except Exception as e:
			self.logger.error(e)

	def create(self, context):
		old_value = int(self.state.get('FlowFileNumber', '0'))
		new_value = old_value + 1
		new_state = {'FlowFileNumber': str(new_value)}
		try:
			context.getStateManager().setState(new_state, Scope.CLUSTER)
			self.state = new_state
		except StateException as e:
			self.logger.warn('Failed to save state. ' + str(e))
		try:
			flowfileContent = self.buildAndRunQuery()
			if flowfileContent:
				return FlowFileSourceResult(relationship = 'success', contents = flowfileContent)
			else:
				self.logger.error("Unable to export date from Splunk. Please verify configuration")
		except Exception as e:
			self.logger.error(e)
