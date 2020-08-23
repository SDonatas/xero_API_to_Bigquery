import requests
import json
import base64
import os
import pickle
import logging
import pandas as pd
import datetime
import calendar
import asyncio

from lib import bigqueryWrapper
from lib import gcloud_bucket
import csv


#environmental path
path = os.path.dirname(os.path.realpath(__name__))

#Logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=path + '/xero.log',level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


endpoints = ['https://api.xero.com/api.xro/2.0/Reports/BalanceSheet', 'https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss']

#StartReportFrom
#days_back = 365 * 10
days_back = 30 * 12
runFrom = datetime.datetime.now().date() - datetime.timedelta(days = days_back)
runFrom = runFrom - datetime.timedelta(days=runFrom.day - 1)

#Initial Credentials dictionary (only use initially)
def initialUploadOfCredentials():

    credentials = {"client_id": "EDF2122A3DBF44CA8D400460DD490CC5",
                   'client_secret': "UuLukubR2PzkN5WaUvq0aE6mO6qwEwsQbp2EadKx49wKzKDl",
                   "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IjFDQUY4RTY2NzcyRDZEQzAyOEQ2NzI2RkQwMjYxNTgxNTcwRUZDMTkiLCJ0eXAiOiJKV1QiLCJ4NXQiOiJISy1PWm5jdGJjQW8xbkp2MENZVmdWY09fQmsifQ.eyJuYmYiOjE1ODQ5MDgxNjEsImV4cCI6MTU4NDkwODQ2MSwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS54ZXJvLmNvbSIsImF1ZCI6IkVERjIxMjJBM0RCRjQ0Q0E4RDQwMDQ2MERENDkwQ0M1IiwiaWF0IjoxNTg0OTA4MTYxLCJhdF9oYXNoIjoibEZpRWtkemVMUWdXY3VGSWVCbjdFdyIsInNpZCI6IjQ5NmQ4YTYxZTQ1ZDA3NGM0NDQ2ZmRkMGNlNjYzYjkxIiwic3ViIjoiZGY4OWU2NTNhZGI5NWJmMjgwZTZiYmRlMTY1Njc2M2YiLCJhdXRoX3RpbWUiOjE1ODQ5MDc1MDIsInhlcm9fdXNlcmlkIjoiYWJmNTdhYWEtY2RkNi00OGMzLWE1MzMtZTIxMDdhYjJmMTI2IiwiZ2xvYmFsX3Nlc3Npb25faWQiOiJiM2U4MTMxMjU3MDQ0ZDBiYTJkNzc2ZjJjZGEzMTA4ZCIsInByZWZlcnJlZF91c2VybmFtZSI6ImFuZHJldy5nZWFyeUBhY2NvcmRwcm9wZXJ0eS5jb20uYXUiLCJlbWFpbCI6ImFuZHJldy5nZWFyeUBhY2NvcmRwcm9wZXJ0eS5jb20uYXUiLCJnaXZlbl9uYW1lIjoiQW5kcmV3IiwiZmFtaWx5X25hbWUiOiJHZWFyeSJ9.YrTW74k0ZT2kl__HMMgcrbLlFQ567SuD_1QpMFCCdmKI_a4JhlGOkUY9v8Lpm2oVvBZnxCrTjQqXv4DnIwr-I1U2jBRH5UEJEVVNWuGeXg5r2Klbh3ApolgH1mlFB3l5M46wMDLO-h-LUn7e1UVySNuzNhdvwdaL0mCYyXCGt8vmH4GujSNyuS0mL-6LxQnF9KplJUEuJRyMbamPh1htK2q83lkTzGm6b8Ovfl5CNpFCn3Dn1qty84Nihlik9Wx3VlHUJkU5QcJuK2zEDplbB2KUZ9yK5sUChptF7D3TAPhMTLUyMNEBNrkF1hQji5CScrqx_bYxfzVYKhdlnCdsdw",
                   'access_token': "eyJhbGciOiJSUzI1NiIsImtpZCI6IjFDQUY4RTY2NzcyRDZEQzAyOEQ2NzI2RkQwMjYxNTgxNTcwRUZDMTkiLCJ0eXAiOiJKV1QiLCJ4NXQiOiJISy1PWm5jdGJjQW8xbkp2MENZVmdWY09fQmsifQ.eyJuYmYiOjE1ODQ5MDgxNjEsImV4cCI6MTU4NDkwOTk2MSwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS54ZXJvLmNvbSIsImF1ZCI6Imh0dHBzOi8vaWRlbnRpdHkueGVyby5jb20vcmVzb3VyY2VzIiwiY2xpZW50X2lkIjoiRURGMjEyMkEzREJGNDRDQThENDAwNDYwREQ0OTBDQzUiLCJzdWIiOiJkZjg5ZTY1M2FkYjk1YmYyODBlNmJiZGUxNjU2NzYzZiIsImF1dGhfdGltZSI6MTU4NDkwNzUwMiwieGVyb191c2VyaWQiOiJhYmY1N2FhYS1jZGQ2LTQ4YzMtYTUzMy1lMjEwN2FiMmYxMjYiLCJnbG9iYWxfc2Vzc2lvbl9pZCI6ImIzZTgxMzEyNTcwNDRkMGJhMmQ3NzZmMmNkYTMxMDhkIiwianRpIjoiMGQxN2YzZmQwZjJkMjIwOTZjMGZmZThjZTBmYzMzZWMiLCJzY29wZSI6WyJlbWFpbCIsInByb2ZpbGUiLCJvcGVuaWQiLCJhY2NvdW50aW5nLnJlcG9ydHMucmVhZCIsIm9mZmxpbmVfYWNjZXNzIl19.mMsNK-wKc8iTbErCRNjwC_WUpOxY35MJHuCjU6qCMY36sirCC5XB7UPyFGK8fQKRqpvcUviK6LBOqS5epe6wh1eTbpvpEoql2JjtuFD4qvwt4SUpS2uEMhDfb-COydJbm-PDEAOug6WLwbg0YOnaDQ7Kl7F8nXPAPvSSdRQcUmkKfBGs65WB3AIq30YfJNBP4LekiDJs_phzggS11LErS8rjXIJ_hidH1JTGZJgBAwwPCISxwAkwFQ3xrJ5FgjmYfmkyucKsUS6DQWPoCGkghKHO1Vv0GuhTllm6nfVLfpZSUtVVLPoC7ZnrlFGeXBynHdcZCRVulK50SlfiZ4iu-g",
                   'refresh_token': "20200e98484f8c2300d8de8f8eaaba6700076de8a50a76e40fc2fff8de2ed4b7"
                  }


    initialBucketUpload = gcloud_storage.gcloud_bucket()
    initialBucketUpload.upload_blob_from_string(json.dumps(credentials))

class credentials(gcloud_bucket):
    def __init__(self):
        super().__init__()
        self.credentials = json.loads(self.download_blob_to_string())
        #self.credentials = json.load(open(credentials_file_gcloud, 'r'))
        #self.credentials = pickle.load(open(credentials_file, 'rb'))
    def updateCredentials(self, jsonPayload):
        assert type(jsonPayload) == dict, "Error: Payload passed is wrong data type"
        self.credentials.update(jsonPayload)
        #pickle.dump(self.credentials, open(credentials_file, 'wb'))
        self.upload_blob_from_string(json.dumps(self.credentials))
        #json.dump(self.credentials, open(credentials_file_gcloud, 'w'))


class parserTools:
    def __init__(self):
        pass

    def generateLookupDates(self, startDate: datetime.date, endDate: datetime.date = None, startAndEnd = False) -> list:

            listOfDates = []

            if endDate == None:
                endDate = datetime.datetime.now().date()
                endDate = datetime.date(endDate.year, endDate.month, calendar.monthrange(endDate.year, endDate.month)[-1])
            else:
                endDate = endDate

            dateLoop = startDate - datetime.timedelta(days = startDate.day - 1)
            
            while dateLoop <= endDate:
                
                if startAndEnd == False:
                    listOfDates.append(datetime.date(dateLoop.year, dateLoop.month, calendar.monthrange(dateLoop.year, dateLoop.month)[-1]))
                else:
                    listOfDates.append( (   datetime.date(dateLoop.year, dateLoop.month, 1), datetime.date(dateLoop.year, dateLoop.month, calendar.monthrange(dateLoop.year, dateLoop.month)[-1])   )   )
                
                def incrementOneMonth(dateInput: datetime.date) -> datetime.date:
                    if dateInput.month == 12:
                        return datetime.date(dateInput.year + 1, 1, 1)
                    else:
                        return datetime.date(dateInput.year, dateInput.month + 1, 1)
                
                dateLoop = incrementOneMonth(dateLoop)


            return listOfDates


        

class xeroAPI(credentials, parserTools):
    def __init__(self):
        super().__init__()
        self.api_url = "https://api.xero.com/api.xro/2.0/"
        self.lastRefresh = None
        
    def checkConnection(self):
        response = requests.get('https://api.xero.com/connections', 
                            headers = {
                                'Authorization': "Bearer " + self.credentials['access_token'],
                                'Content-Type': 'application/json'
                                      })
        
        response = json.loads(response.text)

        #Append tenant_id if not within credentials
        if 'tenantId' not in self.credentials.keys() and type(response) == list and 'tenantId' in response[0].keys():
            self.credentials.update({'tenantId': response[0]['tenantId']})


        if type(response) == list:
            logger.info(response)
            return True
        else:
            logger.info(response)
            return False

    def tokenRefreshTimeoutCheck(self):
        if self.lastRefresh == None:
            return True
        elif (datetime.datetime.now() - self.lastRefresh).seconds <= 1500:
            return False
        else:
            return True
    
    def getReport(self, url: str, params: dict) -> list:
        #Check if connection works, if not refresh token        
        if self.tokenRefreshTimeoutCheck() == True:
            if self.checkConnection() == False:
                self.refreshAccessToken()
        
        #Get report
        headers = {'Authorization': "Bearer " + self.credentials['access_token'],
                    'Accept': 'application/json',
                    'Xero-tenant-id': self.credentials['tenantId']}

        response = requests.get(self.api_url + url, headers = headers, params = params)
        
        logger.info(response.url)

        #print(response.text)
        response = json.loads(response.text)
        #print(response)
        
        try:
            assert url.split("/")[0] in response.keys(), "API did not return any {} data".format(url)
        
            #pickle.dump(pd.DataFrame.from_dict(response), open('invoice_dump.pkl', 'wb'))
            #response = pd.DataFrame.from_dict([x for x in response['Invoices']])
            response = [x for x in response[url.split("/")[0]]]
            #pickle.dump(response, open('invoice_dump.pkl', 'wb'))
            #logger.info('Total {} pulled {}'.format(url, len(response)))

            return response

        except Exception as e:
            logger.info("Errro: {}".format(e))
            return []

    def getProfitAndLoss(self, startDate: datetime.date, endDate: datetime.date = datetime.datetime.now().date()):

        def parseProfitAndLoss(inputData):
            header = []
            data = []
            dates = inputData[0]['ReportTitles'][2].split(" to ")
            dates = [datetime.datetime.strptime(d, "%d %B %Y").strftime("%Y-%m-%d") for d in dates]
            header.extend(dates)
            del dates
            
            for row in inputData[0]['Rows']:
                if row['RowType'] == 'Section':
                    for innerRow in row['Rows']:
                        data.append([*header, row["Title"], innerRow['Cells'][0]['Value'], innerRow['Cells'][1]['Value']])
            
            return data  

        
        def incomeStatement(startDate, endDate):
            params = {'fromDate': startDate.strftime("%Y-%m-%d"), 'toDate': endDate.strftime("%Y-%m-%d")}
            response = self.getReport('Reports/ProfitAndLoss', params)
            return response


        lookupDates = self.generateLookupDates(startDate, endDate, True)

        for dateTuple in lookupDates:
            response = incomeStatement(dateTuple[0], dateTuple[1])
            response = parseProfitAndLoss(response)
            yield response


    def getbalanceSheet(self, startDate: datetime.date):

        def parseBalanceSheet(inputData):
            
            assert type(inputData) == list, 'Wrong type passed to Balance Sheet parser, expected list'
            assert type(inputData[0]) == dict and 'Rows' in inputData[0].keys(), 'Wrong inner type or no Rows key within Balance Sheet data'
            
            #get header
            assert 'RowType' in inputData[0]['Rows'][0].keys() and inputData[0]['Rows'][0]['RowType'] == 'Header', 'Could not parse Balance Sheet Header'
            
            header = [datetime.datetime.strptime(x['Value'], "%d %b %Y").strftime("%Y-%m-%d") for x in inputData[0]['Rows'][0]['Cells'][1:]]
            """
            header.extend([datetime.datetime.strptime(header[0], "%d %b %Y").year,
                        datetime.datetime.strptime(header[0], "%d %b %Y").month,
                        datetime.datetime.strptime(header[1], "%d %b %Y").year,
                        datetime.datetime.strptime(header[1], "%d %b %Y").month,
                        str(datetime.datetime.strptime(header[0], "%d %b %Y").year) + "-" + str(datetime.datetime.strptime(header[0], "%d %b %Y").month),
                        str(datetime.datetime.strptime(header[1], "%d %b %Y").year) + "-" + str(datetime.datetime.strptime(header[0], "%d %b %Y").month)
                        ])
            """
            
            data = []
            
            for row in inputData[0]['Rows'][1:]:
                
                innerdata = []
                
                if len(row['Rows']) > 0:
                    for innerRow in row['Rows']:
                        innerdata = []
                        innerdata.extend(header)
                        innerdata.extend([row['Title']])
                        innerdata.extend([x['Value'] for x in innerRow['Cells']])
                        data.append(innerdata)
                else:
                    data.append([*header, row['Title'], None, None, None])
                    
                
            return data
        
        

        def GetendOfMonth(date: datetime.date) -> list: 

            params = {'date': date.strftime("%Y-%m-%d")}
            response = self.getReport('Reports/BalanceSheet', params)
            return response


        lookupDates = self.generateLookupDates(startDate)

        for date_lookup in lookupDates:
            response = GetendOfMonth(date_lookup)
            response = parseBalanceSheet(response)
            yield response


    
    
    def getInvoice(self):
        if self.checkConnection() == False:
            self.refreshAccessToken()

        url = 'https://api.xero.com/api.xro/2.0/Invoices'
        
        response = requests.get(url,
                     headers = {
                         'Authorization': 'Bearer ' + self.credentials['access_token'],
                         'Accept': 'application/json',
                         'Xero-tenant-id': self.credentials['tenantId'],
                         'If-Modified-Since': (datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S")
                               },
                     params = {'where': 'Type=="ACCREC"'}
                    
                    )
        
        #print(response.url)
        response = json.loads(response.text)
        assert 'Invoices' in response.keys(), "API did not return any invoice data"
        #pickle.dump(pd.DataFrame.from_dict(response), open('invoice_dump.pkl', 'wb'))
        #response = pd.DataFrame.from_dict([x for x in response['Invoices']])
        response = [x for x in response['Invoices']]
        #pickle.dump(response, open('invoice_dump.pkl', 'wb'))
        logger.info('Total Invoices pulled {}'.format(len(response)))

        return response
    
    
    
    def refreshAccessToken(self):
        url = 'https://identity.xero.com/connect/token'
        
        response = requests.post(url,
                                headers = {
                                    'Authorization': "Basic " + base64.b64encode(bytes(self.credentials['client_id'] + ":" + self.credentials['client_secret'], 'utf-8')).decode('utf-8')},
                                data = {
                                    'grant_type': 'refresh_token',
                                    'refresh_token': self.credentials['refresh_token']
                                })
        
        
        response = json.loads(response.text)
        logger.info(response)
        self.updateCredentials(response)
        self.lastRefresh = datetime.datetime.now()
        


async def runReport(reportName):

    #delete old payload from database column number used for removing ToDate data
    col_number = {"ProfitAndLoss": 1,
                  "balanceSheet": 0
                 }
    
    #function to delete files
    def deleteFile(filename):
        try:
            os.remove(filename)
        except:
            pass

    #Get response
    xeroInstance = xeroAPI()
    response = getattr(xeroInstance, "get" + reportName)(runFrom)
    await asyncio.sleep(1)

    #Initiate database
    db = bigqueryWrapper()
    db.settings['table'] = reportName
    #db.AddTable()

    #start uploading data
    for key, data in enumerate(response):
        filename = "/tmp/{}_{}.csv".format(reportName, key)
        deleteFile(filename)

        #save payload
        with open(filename, 'w') as writeFile:
            wr = csv.writer(writeFile)
            wr.writerows(data)
        
        #remove old files
        db.deleteLoad('ToDate', list(set(["'" + x[col_number[reportName]] + "'" for x in data])))
        db.load_data_from_file(filename)



async def main():
    await asyncio.gather(runReport("balanceSheet"), runReport("ProfitAndLoss"))
    #await asyncio.gather(runReport("ProfitAndLoss"))

def accord_xero(*args):
    try:
        asyncio.run(main())
        postLog({"ScriptName": "Xero", "SuccessfulRun": True, "Details": "BalanceSheet + ProfitLoss", "Timestamp": datetime.datetime.now()})
    except:
        postLog({"ScriptName": "Xero": "SuccessfulRun": False, "Details": "BalanceSheet + ProfitLoss", "Timestamp": datetime.datetime.now()})


def postLog(payload: dict):
    db = bigqueryWrapper()
    db.settings['table'] = "Logs"
    #db.AddTable()
    db.loadRows([payload])


#if __name__ == '__main__':
#    runXero()

