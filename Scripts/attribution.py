#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
from io import BytesIO
import urllib.request
import urllib.parse
import json
import pandas as pd
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
report_start_date=(datetime.datetime.now()- datetime.timedelta(12)).strftime("%Y%m%d")
report_end_date=(datetime.datetime.now()- datetime.timedelta(6)).strftime("%Y%m%d")

try:
	if os.path.exists(file_path):
		pass
	else:
		os.mkdir(file_path)
except OSError:
	print ("Creation of the directory %s failed" % path)

class AmazonAds:
	def __init__(self):
		self.logger = log_handler(__name__ + '.' + self.__class__.__name__)
		try:
				with open('resources/Amz_Cred.json', 'r') as f:
					self.logger.info("script execution has been strated at :"+str(cur_date))
					cred = load(f)
					self.client_id = cred['client_id']
					self.client_secret = cred['client_secret']
					self.profileId = cred['profileId']
					self.endpoint = cred['endpoint']
					self.version = cred['version']
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

	def publishers(self):
		try:
			#url = 'https://advertising-api.amazon.com/attribution/publishers'
			results=[]
			response = requests.get(f'{self.endpoint}/attribution/publishers', headers=self.headers)
			publishers = response.json()
			for key,value in publishers.items():
			    for data in value:
			        new_dict={}
			        for sub_key,sub_value in data.items():
			            new_dict[sub_key]=sub_value
			        results.append(new_dict)
			publishers_df= pd.DataFrame(results)
			publishers_df.to_csv (file_path+'attribution_publisher.csv',encoding="utf-8-sig",index=False)
			self.logger.exception("Attribution publishers data has been fetched successfully")
			#return publishers_df
		except Exception as e:
			self.logger.exception("Error in getting attribution publishers data")
	def advertisers(self):
		try:
			#url = 'https://advertising-api.amazon.com/attribution/advertisers'
			results=[]
			response = requests.get(f'{self.endpoint}/attribution/advertisers', headers=self.headers)
			advertisers = response.json()
			for key,value in advertisers.items():
			    for data in value:
			        new_dict={}
			        for sub_key,sub_value in data.items():
			            new_dict[sub_key]=sub_value
			        results.append(new_dict)
			advertisers_df= pd.DataFrame(results)
			advertisers_df.to_csv (file_path+'attribution_advertisers.csv',encoding="utf-8-sig",index=False)
			self.logger.exception("Attribution advertisers data has been fetched successfully")
		except Exception as e:
			self.logger.exception("Error in getting attribution advertisers data")


	def reports(self):
		try:
		    r = requests.post(
		        f'{self.endpoint}/attribution/report',
		        json={
		          "reportType": "PERFORMANCE",
		          #"dimensions": ['campaignId', 'adgroupId', 'adgroupId'],
				  "endDate": report_end_date,
				  "startDate": report_start_date,
			      "count":10000 ,
		          "metrics": ",".join(['Click-throughs','attributedDetailPageViewsClicks14d','attributedAddToCartClicks14d','attributedPurchases14d','unitsSold14d','attributedSales14d','attributedTotalDetailPageViewsClicks14d','attributedTotalAddToCartClicks14d','attributedTotalPurchases14d','totalUnitsSold14d','totalAttributedSales14d'
		              ]),
		        },
		        headers=self.headers,
		    )
		    report_content = r.json()
		    results=[]
		    for key,value in report_content.items():
		    	if key == 'reports':
		    		for data in value:
		    			new_dict={}
		    			for sub_key,sub_value in data.items():
		    				new_dict[sub_key]=sub_value
		    			results.append(new_dict)
		    report_df= pd.DataFrame(results)
		    report_df.to_csv (file_path+'attribution_performace_report.csv',encoding="utf-8-sig",index=False)
		    self.logger.exception("Attribution performance report has been fetched successfully")
		except Exception as e:
			self.logger.exception("Error in getting attribution performance report data")
		try:
		    r = requests.post(
				f'{self.endpoint}/attribution/report',
				json={
				"reportType": "PRODUCTS",
				"endDate": report_end_date,
				"startDate": report_start_date,
				"count":10000 ,
				"metrics": ",".join(['attributedDetailPageViewsClicks14d','attributedAddToCartClicks14d','attributedPurchases14d','unitsSold14d','attributedSales14d']),
				},
				headers=self.headers,
				)
		    report_content = r.json()
		    results=[]
		    for key,value in report_content.items():
		    	if key == 'reports':
		    		for data in value:
		    			new_dict={}
		    			for sub_key,sub_value in data.items():
		    				new_dict[sub_key]=sub_value
		    			results.append(new_dict)
		    report_df= pd.DataFrame(results)
		    report_df.to_csv (file_path+'attribution_product_report.csv',encoding="utf-8-sig",index=False)
		    self.logger.exception("Attribution products report has been fetched successfully")
		except Exception as e:
			self.logger.exception("Error in getting attribution products report data")
Amz=AmazonAds()
Amz.publishers()
Amz.advertisers()
Amz.reports()

elapsed_time_secs=("script execution time :"+str(datetime.datetime.now()-start_time))
print(elapsed_time_secs) #It returns time in sec