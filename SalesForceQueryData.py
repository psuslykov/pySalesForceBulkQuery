__author__ = 'Pavlo_Suslykov'

import sys, os, traceback
import urllib.request
import csv, gzip
import re, xml.etree.ElementTree as ET
from time import sleep

class SalesForceBulkAPI:

    http_status_codes=[200,201,202,203,204]

    def __init__(self, objectname, parameters, logger=None, start_date='', end_date=''):
        """The constructor gets parameter file and parses the custom settings
        for object that is loaded from SalesForce"""
        #Set up the logger
        self.logger = logger
        self.logger.info("===================================================================")
        self.logger.info("    Initialisation of parameters for module %s",__name__)
        self.logger.info("===================================================================")
        self.object_name=objectname
        self.start_date=''
        self.end_date=''
        try:
            param_tree=ET.parse(parameters)
            self.query_file=param_tree.find("./*[@name='"+self.object_name+"']/query").text
            self.output_file=param_tree.find("./*[@name='"+self.object_name+"']/outfile").text
            self.load_type=param_tree.find("./*[@name='"+self.object_name+"']/loadtype").text
            self.logger.debug("Initialised parameters: \n\t query_file: [%s] \n\t output_file: [%s] \n\t load_type: [%s]",
                              self.query_file, self.output_file, self.load_type)
        except:
            self.logger.error("The error during getting parameters for object '%s' from the parameter file %s.", self.object_name, parameters)
            self.logger.error(traceback.format_exc())
            raise

    def login(self):
        """Logging into SalesForce"""
        headers = {"Content-Type": "text/xml; charset=UTF-8", "SOAPAction": "login"}
        url = "https://login.salesforce.com/services/Soap/u/37.0"
        self.logger.info("Login into SalesForce service: %s", url)
        try:
            login_parameters = open("sforce_login.xml").read().encode('utf-8')
            request = urllib.request.Request(url=url, data=login_parameters, headers=headers)
            response = urllib.request.urlopen(request)
            response_text=response.read().decode('utf-8')
            self.logger.debug("Response: \n\t %s", response_text)
            if response.getcode() != 200 :
                self.logger.warning("Login failed with status: %d", response.getcode())
                self.logger.warning(response_text)
                self.logger.error("Finishing the execution due to exception during the session creation in SalesForce.")
                sys.exit(response.getcode())
            response_login = ET.fromstring(response_text)

            self.session_id = response_login.find('{http://schemas.xmlsoap.org/soap/envelope/}Body/{urn:partner.soap.sforce.com}loginResponse/{urn:partner.soap.sforce.com}result/{urn:partner.soap.sforce.com}sessionId').text
            serverUrl = response_login.find('{http://schemas.xmlsoap.org/soap/envelope/}Body/{urn:partner.soap.sforce.com}loginResponse/{urn:partner.soap.sforce.com}result/{urn:partner.soap.sforce.com}serverUrl').text
            self.instance = serverUrl.split("/")[2]
        except AttributeError:
            self.logger.error("The error during getting SessionId and Instance from SalesForce reply: \n\t %s", response_login)
            self.logger.error(traceback.format_exc())
            raise
        except urllib.error.HTTPError as e:
            self.logger.error("HTTP Error: \n\t %s",e.read())
            raise
        except:
            self.logger.error("Unknown error:")
            self.logger.error(traceback.format_exc())
            raise
        self.logger.info("The session was created.")
        self.logger.info("Instance: %s \n\t Session Id: %s", self.instance, self.session_id)


    def createJob(self):
        """Declaration of new job in SalesForce"""
        headers = {"X-SFDC-Session": self.session_id, "Content-Type": "application/xml; charset=UTF-8"}
        url = "https://"+self.instance+"/services/async/30.0/job"
        self.logger.info("Creating job: %s", url)
        try:
            job_parameters = open("sforce_job.xml").read().replace("[OBJECT_NAME]",self.object_name,1).encode('utf-8')
            request = urllib.request.Request(url=url, data=job_parameters, headers=headers)
            response = urllib.request.urlopen(request)
            response_text=response.read().decode('utf-8')
            self.logger.debug("Response: \n\t %s", response_text)
            if not response.getcode() in self.http_status_codes:
                self.logger.warning("Creating job failed with status: %d. \n\t Response: \n\t %s", response.getcode(), response_text)
                self.logger.error("Finishing the execution due to exception during the job creation in SalesForce.")
                sys.exit(response.getcode())
            response_job = ET.fromstring(response_text)

            self.job_id=response_job.find('{http://www.force.com/2009/06/asyncapi/dataload}id').text
        except AttributeError:
            self.logger.error("The error during getting JobId from SalesForce reply: \n\t %s", response_job)
            self.logger.error(traceback.format_exc())
            raise
        except urllib.error.HTTPError as e:
            self.logger.error("HTTP Error: \n\t %s",e.read())
            raise
        except:
            self.logger.error("Unknown error:")
            self.logger.error(traceback.format_exc())
            raise
        self.logger.info("The job was created with the JobId: %s", self.job_id)

    def addBatchToJob(self):
        """Adding the job into batch and kicks it off"""
        headers = {"X-SFDC-Session": self.session_id, "Content-Type": "text/csv; charset=UTF-8"}
        url = "https://"+self.instance+"/services/async/30.0/job/" + self.job_id + "/batch"
        try:
            job_sql = open(self.query_file).read().replace("[START_DATETIME]",self.start_date,1).replace("[END_DATETIME]",self.end_date,1)
            self.logger.info("Adding the query to batch: %s", url)
            self.logger.debug('Query: [%s]', job_sql.replace('\n',''))

            request = urllib.request.Request(url=url, data=job_sql.encode('utf-8'), headers=headers)
            response = urllib.request.urlopen(request)

            if not response.getcode() in self.http_status_codes:
                self.logger.warning("Adding the job to batch failed with status: %d", response.getcode())
                self.logger.warning(response.read())
                self.logger.error("Finishing the execution due to exception during the adding job to batch in SalesForce.")
                sys.exit(response.getcode())
            response_text=response.read().decode('utf-8')
            self.logger.debug("Response: \n\t %s", response_text)
            response_batch = ET.fromstring(response_text)

            self.batch_id=response_batch.find('{http://www.force.com/2009/06/asyncapi/dataload}id').text
        except AttributeError:
            self.logger.error("The error during getting JobId from SalesForce reply: \n\t %s", response_batch)
            self.logger.error(traceback.format_exc())
            raise
        except urllib.error.HTTPError as e:
            self.logger.error("HTTP Error: \n\t %s",e.read())
            raise
        except:
            self.logger.error("Unknown error:")
            self.logger.error(traceback.format_exc())
            raise
        self.logger.info("The batch was created with the BatchId: %s", self.batch_id)


    def checkBatchStatus(self):
        """Checking the status of the batch"""
        self.logger.info("Checking the status of batch: %s", self.batch_id)
        headers = {"X-SFDC-Session": self.session_id}
        url = "https://"+self.instance+"/services/async/30.0/job/" + self.job_id + "/batch/"+self.batch_id
        try:
            request = urllib.request.Request(url=url, headers=headers)
            response = urllib.request.urlopen(request)
            response_text=response.read().decode('utf-8')
            self.logger.debug("Response: \n\t %s", response_text)
            if not response.getcode() in self.http_status_codes:
                self.logger.warning("Checking the batch status failed with the code error: %d.\n\tResponse: \n\t%s", response.getcode(), response_text)
            response_check = ET.fromstring(response_text)

            batch_status=response_check.find('{http://www.force.com/2009/06/asyncapi/dataload}state').text
            if batch_status=='Failed':
                self.logger.warning("Batch status is Failed: \n\t %s", response_text)
                self.logger.error("Finishing the execution due to failure of batch.")
                sys.exit(1)
        except AttributeError:
            self.logger.warning("The error during getting batch status from SalesForce reply: \n\t %s", response_check)
            self.logger.error(traceback.format_exc())
        except urllib.error.HTTPError as e:
            self.logger.error("HTTP Error: \n\t %s",e.read())
        except:
            self.logger.error("Unknown error:")
            self.logger.error(traceback.format_exc())
            raise
        return batch_status

    def getQueryResultID(self):
        """Get ID record with result set."""
        self.logger.info("Getting the QueryResultId")
        headers = {"X-SFDC-Session": self.session_id}
        url = "https://"+self.instance+"/services/async/30.0/job/" + self.job_id + "/batch/"+self.batch_id+"/result"
        #response = requests.get(url=url, headers = headers, verify=False)
        try:
            request = urllib.request.Request(url=url, headers=headers)
            response = urllib.request.urlopen(request)
            response_text=response.read().decode('utf-8')
            self.logger.debug("Response: \n\t %s", response_text)
            if not response.getcode() in self.http_status_codes:
                self.logger.warning("Creating job failed with status: %d", response.getcode())
                self.logger.warning(response_text)
                self.logger.error("Finishing the execution due to exception during the getting ResultId from SalesForce.")
                sys.exit(response.getcode())
            response_result = ET.fromstring(response_text)

            self.result_id=[]
            res=response_result.findall('{http://www.force.com/2009/06/asyncapi/dataload}result')
            for r in res:
                self.result_id.append(r.text)
        except AttributeError:
            self.logger.error("The error during getting JobId from SalesForce reply: \n\t %s", response_result)
            self.logger.error(traceback.format_exc())
            raise
        except urllib.error.HTTPError as e:
            self.logger.error("HTTP Error: \n\t %s",e.read())
            raise
        except:
            self.logger.error("Unknown error:")
            self.logger.error(traceback.format_exc())
            raise
        self.logger.info("ResultId: %s", self.result_id)

    def downloadResultFile(self):
        """Download prepared data from the SalesForce"""
        filename_start_date=self.start_date
        filename_end_date=self.end_date
        for ch in ['.',':','-',' ']:
            filename_start_date=filename_start_date.replace(ch, '')
            filename_end_date=filename_end_date.replace(ch,'')
        self.output_file = self.output_file.replace("[START_DATETIME]",filename_start_date,1).replace("[END_DATETIME]",filename_end_date,1)
        self.logger.info("Download prepared data into file: %s", self.output_file)
        headers = {"X-SFDC-Session": self.session_id}

        datafile=open(file=self.output_file, mode="bw")
        chunk_size = 1024 * 1024 * 10

        for res_id in self.result_id:
            url = "https://"+self.instance+"/services/async/30.0/job/" + self.job_id + "/batch/"+self.batch_id+"/result/"+res_id
            try:
                request = urllib.request.Request(url=url, headers=headers)
                response = urllib.request.urlopen(request)

                if not response.getcode() in self.http_status_codes:
                    self.logger.warning("Datafile load failed with error: %d.\n\t Response:\n\t %s",response.status_code, response.text)
                    self.logger.error("Finishing the execution due to exception during the getting ResultId from SalesForce.")
                    sys.exit(response.getcode())

                while True:
                    chunk=response.read(chunk_size)
                    if not chunk: break
                    datafile.write(chunk)
                    datafile.flush()
                    os.fsync(datafile)
            except urllib.error.HTTPError as e:
                self.logger.error("HTTP Error: \n\t %s",e.read())
                raise
            except:
                self.logger.error("Unexpected error while saving the datafile: %s",self.output_file)
                self.logger.error(traceback.format_exc())
                raise
        self.logger.info("Data file was loaded from the SalesForce")
        response.close()


    def convertForEDW(self):
        self.logger.info("Convert the datafile %s to Hadoop format")
        try:
            out = gzip.open(self.output_file+'.gz',"wt", encoding='utf-8')
            with open(self.output_file, encoding='utf-8') as csvfile:
                firstLine = True
                self.logger.info("Open CSV file %s for conversion", csvfile.name)
                streamreader = csv.reader(csvfile, dialect='excel')
                for line in streamreader:
                    if firstLine:
                        firstLine=False
                        continue
                    for index, item in enumerate(line):
                        value = str(item).replace('\n','')
                        if re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.000Z',value):
                            value = value.replace('T', ' ').replace('.000Z','')
                        line.pop(index)
                        line.insert(index, value)
                    out.write(';'.join(line)+'\n')
        except:
            self.logger.error("Unexpected error while converting file: %s", self.output_file)
            self.logger.error(traceback.format_exc())
            raise
        self.logger.info("Conversion completed")
        out.close()

    def loadDataForEDW(self, start_date='', end_date=''):
        if self.load_type == 'incremental':
            if start_date==False or end_date==False:
                self.logger.error("For object '%s' the Start and End dates should be defined")
                sys.exit(1)
            else:
                self.start_date=start_date
                self.end_date=end_date
                self.logger.info("The 'Start' and 'End' dates defined as: %s and %s", self.start_date, self.end_date)

        self.login()
        self.createJob()
        self.addBatchToJob()
        batch_status=self.checkBatchStatus()
        while batch_status!= 'Completed':
            self.logger.info("The batch status is: %s. Waiting 60 sec...", batch_status)
            sleep(60)
            batch_status=self.checkBatchStatus()
        self.getQueryResultID()
        self.downloadResultFile()
        self.convertForEDW()