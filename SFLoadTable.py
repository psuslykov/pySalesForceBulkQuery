__author__ = 'Pavlo_Suslykov'
from SalesForceQueryData import SalesForceBulkAPI
from optparse import OptionParser
import logging, traceback
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# Get input parameters
parser = OptionParser()
parser.add_option("-o", "--object", dest="object_name", type="string",
                  help="Define SalesForce object name that should be loaded")
parser.add_option("-l", "--logLevel", dest="log_level", type="string",
                  help="Define logging level. INFO, DEBUG, ERROR")
(options, args) = parser.parse_args()

# Setup the logger
log_formater=logging.Formatter(fmt='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
console_log_handler = logging.StreamHandler()
console_log_handler.setFormatter(log_formater)
file_log_handler = logging.FileHandler("logs\SFLoadTable_%s.log"%options.object_name)
file_log_handler.setFormatter(log_formater)
logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_log_handler)
logger.addHandler(file_log_handler)

incremental_objects = ['Case', 'Case_Time__c', 'Guest__c', 'Time_Log__c', 'Account', 'CaseHistory']
try:
    # Get Start and End dates
    param_tree=ET.parse("dates.xml")
    if options.object_name in incremental_objects:
        param_load_datetime=param_tree.find("./*[@name='"+options.object_name+"']/loadeddatetime")
        end_date=param_load_datetime.text

        if end_date is None:
            start_date='1999-01-01T00-00-00.000Z'
        else:
            start_date=datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S.000Z')-timedelta(minutes=1)
            #Format date to SalesForce
            start_date=datetime.strftime(start_date, '%Y-%m-%dT%H:%M:%S.000Z')
        end_date=datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.000Z')
    else:
        start_date=''
        end_date=''

    # Load datafile
    sf = SalesForceBulkAPI(objectname=options.object_name,parameters="parameters.xml", logger=logger)
    sf.loadDataForEDW(start_date=start_date, end_date=end_date)

    # Save loaded date into file
    if options.object_name in incremental_objects:
        param_load_datetime.text=end_date
        param_tree.write("dates.xml")
except:
    logger.error("Unexpected Error in main file.")
    logger.error(traceback.format_exc())