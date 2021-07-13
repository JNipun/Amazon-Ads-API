#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import json
import gzip
import requests
from io import BytesIO
import urllib.request
import urllib.parse
import pandas as pd
import ast
import datetime
import time
import os,sys
from utils.utils import log_handler
from json import load

cur_date= datetime.datetime.now()
dt = cur_date.strftime("%Y%m%d")
path=os.getcwd()
file_path= path+"/Data/"
start_time = datetime.datetime.now()
report_date=(datetime.datetime.now()- datetime.timedelta(1)).strftime("%Y%m%d")

try:
    if os.path.exists(file_path):
        pass
    else:
        os.mkdir(file_path)
except OSError:
    print ("Creation of the directory %s failed" % path)


class SponseredPortfolios:
    def __init__(self):
        self.logger = log_handler(__name__ + '.' + self.__class__.__name__)
        try:
                with open('resources/Amz_Seller_Cred.json', 'r') as f:
                    self.logger.info('*************************')
                    self.logger.info("SponseredPortfolios execution has been strated at :"+str(cur_date) +"\n")
                    cred = load(f)
                    self.client_id = cred['client_id']
                    self.client_secret = cred['client_secret']
                    self.profileId = cred['profileId']
                    self.endpoint = 'https://'+ str(cred['endpoint'])
                    self.version = 'v2'
                    self.refresh_token = cred['refresh_token']
                    self.logger.info('Amazon Ads Credentails has been loaded')
                self.logger.info('Getting Access toekn From Amazon AdAccount')
                toekn_headers = {
                     'Content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
                }
                #here authorization_data variable used to get all config values and helps to genearte automated token
                authorization_data = "grant_type=refresh_token&client_id="+self.client_id+"&refresh_token="+self.refresh_token+"&client_secret="+self.client_secret
                response = requests.post('https://api.amazon.com/auth/o2/token', headers=toekn_headers, data=authorization_data)
                token=response.json()
                access_token_value=token['access_token']
                self.access_token= access_token_value
                self.headers = {'Authorization': 'Bearer {}'.format(self.access_token),
                            'Amazon-Advertising-API-ClientId': self.client_id,
                            'Amazon-Advertising-API-Scope': self.profileId,
                            'Content-Type': 'application/json'}
                self.logger.info('Access toekn has been generated successfully')
            
        except Exception as e:
                self.logger.exception("Error in loading Amazon Ads Credentails file")

    def Portfolios(self):
        try:
            results=[]
            response = requests.get(f'{self.endpoint}/{self.version}/portfolios/extended', headers=self.headers)
            portfolios = response.json()
            for item in portfolios:
                new_dict={}
                data= item
                for portfolios_key,portfolio_value in data.items():
                    new_dict[portfolios_key]=portfolio_value
                results.append(new_dict)
            portfolios_ex_df= pd.DataFrame(results)
            self.logger.exception("Portfolios extended data has been fetched successfully")
        except Exception as e:
            self.logger.exception("Error in fetching Portfolios extended data")

        try:
            results=[]
            response = requests.get(f'{self.endpoint}/{self.version}/portfolios', headers=self.headers)
            portfolios = response.json()
            for item in portfolios:
                new_dict={}
                data = dict(item)
                for portfolios_key,portfolio_value in data.items():
                    #print(portfolios_key,portfolio_value)
                    new_dict[portfolios_key]=portfolio_value
                results.append(new_dict)
            portfolios_df= pd.DataFrame(results)
            final_portfolios_df=pd.concat([portfolios_ex_df,portfolios_df], axis=0, ignore_index=True)
            final_portfolios_df.to_csv (file_path+'portfolios.csv',index=False)
            self.logger.exception("Portfolios data have been fetched successfully")
        except Exception as e:
            self.logger.exception("Error in fetching Portfolios data")

port=SponseredPortfolios()
port.Portfolios()
elapsed_time_secs=("script execution time :"+str(datetime.datetime.now()-start_time))
print(elapsed_time_secs) #It returns time in sec