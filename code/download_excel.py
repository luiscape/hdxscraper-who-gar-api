# Script to download the Excel file
# data from the WHO Ebola API.

import requests
import scraperwiki

def downloadFile:
		'http://apps.who.int/gho/athena/xmart/data.xls?target=EBOLA_MEASURE/CASES,DEATHS&format=xml&profile=excel&filter=COUNTRY:GIN;COUNTRY:UNSPECIFIED;COUNTRY:LBR;COUNTRY:UNSPECIFIED;COUNTRY:LBR;COUNTRY:UNSPECIFIED;COUNTRY:SLE;COUNTRY:UNSPECIFIED;LOCATION:-;DATAPACKAGEID:2015-01-20;INDICATOR_TYPE:SITREP_CUMULATIVE;INDICATOR_TYPE:SITREP_CUMULATIVE_21_DAYS;SEX:-'


# sw error handler