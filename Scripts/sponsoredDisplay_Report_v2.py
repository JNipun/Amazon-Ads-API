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

#change the value as per you requirment, i have passed date for automation perspective
report_date=(datetime.datetime.now()- datetime.timedelta(3)).strftime("%Y%m%d")

try:
    if os.path.exists(file_path):
        pass
    else:
        os.mkdir(file_path)
except OSError:
    print ("Creation of the directory %s failed" % path)


class AmazonSdReport:
    def __init__(self):
        self.logger = log_handler(__name__ + '.' + self.__class__.__name__)
        try:
                with open('resources/Amz_Seller_Cred.json', 'r') as f:
                    self.logger.info('*************************')
                    self.logger.info("sponseredDisplay Reports execution has been strated at :"+str(cur_date) +"\n")
                    cred = load(f)
                    self.client_id = cred['client_id']
                    self.client_secret = cred['client_secret']
                    self.profileId = cred['profileId']
                    self.endpoint = 'https://'+ str(cred['endpoint'])
                    self.version = 'v2'
                    self.refresh_token = cred['refresh_token']
                    self.advertise = 'sd'
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
    def report_camp(self):
        recordType = "campaigns"
        tactic = [ 'T00001', 'T00020', 'T00030', 'remarketing']
        report_content_final=[]
        try:
            for tc in tactic:
                if tc != 'T00001':
                    r = requests.post(
                        f'{self.endpoint}/{self.version}/{self.advertise}/{recordType}/report',
                        json={
                          "reportDate": report_date,  #YYYYMMDD
                          "tactic": tc,
                          "metrics": ",".join([
                          "campaignName","campaignId","campaignStatus","impressions","clicks","cost","currency","attributedConversions1d","attributedConversions7d","attributedConversions14d","attributedConversions30d","attributedConversions1dSameSKU","attributedConversions7dSameSKU","attributedConversions14dSameSKU","attributedConversions30dSameSKU","attributedUnitsOrdered1d","attributedUnitsOrdered7d","attributedUnitsOrdered14d","attributedUnitsOrdered30d","attributedSales1d","attributedSales7d","attributedSales14d","attributedSales30d","attributedSales1dSameSKU","attributedSales7dSameSKU","attributedSales14dSameSKU","attributedSales30dSameSKU","attributedOrdersNewToBrand14d","attributedSalesNewToBrand14d","attributedUnitsOrderedNewToBrand14d"
                          ]),
                        },
                        headers=self.headers,
                    )
                    #r.raise_for_status()
                    r = r.json()
                    self.logger.info(str(r))
                    reportId = r["reportId"]

                    while r['status'] == 'IN_PROGRESS':
                        r = requests.get(
                            f'{self.endpoint}/v1/reports/{reportId}',
                            headers=self.headers,
                        )
                        r = r.json()
                        print(r)
                    self.logger.info('\n'+ str(r) +'\n')
                    assert r['status'] == 'SUCCESS'

                    location = r["location"]
                    r = requests.get(location, headers=self.headers, allow_redirects=False)
                    location = r.headers["Location"]

                    # download the report. do not send special headers as this is a regular
                    # call to S3
                    report = requests.get(location)
                    report_content=gzip.decompress(report.content)
                    report_content=json.loads(report_content)   
                    report_content_final.append(report_content)
                else:
                    r = requests.post(
                        f'{self.endpoint}/{self.version}/{self.advertise}/{recordType}/report',
                        json={
                          "reportDate": report_date,  #YYYY,MMDD
                          "tactic": tc,
                          "metrics": ",".join([
                            'campaignName','campaignId','campaignStatus','currency','impressions','clicks','cost','attributedDPV14d','attributedUnitsSold14d','attributedSales14d'
                          ]),
                        },
                        headers=self.headers,
                    )
                    #r.raise_for_status()
                    r = r.json()
                    self.logger.info(str(r))
                    reportId = r["reportId"]

                    while r['status'] == 'IN_PROGRESS':
                        r = requests.get(
                            f'{self.endpoint}/v1/reports/{reportId}',
                            headers=self.headers,
                        )
                        r = r.json()
                        print(r)
                    self.logger.info('\n'+ str(r) +'\n')
                    assert r['status'] == 'SUCCESS'

                    location = r["location"]
                    r = requests.get(location, headers=self.headers, allow_redirects=False)
                    location = r.headers["Location"]

                    # download the report. do not send special headers as this is a regular
                    # call to S3
                    report = requests.get(location)
                    report_content=gzip.decompress(report.content)
                    report_content=json.loads(report_content)   
                    report_content_final.append(report_content)

            results=[]
            for item in report_content_final:
                if type(item) is list and len(item) != 0:
                    for camp in item: 
                        results.append(camp)
            report_df= pd.DataFrame(results)
            report_df['campaignType'] = 'sponseredDisplay'
            report_df['extractionDate'] = report_date
            report_df.to_csv (file_path+'sponseredDisplay_campaign_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredDisplay campaign report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredDisplay campaign report data")

    def report_camp_ad_group(self):
        recordType = "adGroups"
        tactic = ['T00020', 'T00030', 'remarketing' ]
        report_content_final=[]
        try:
            for tc in tactic:
                r = requests.post(
                    f'{self.endpoint}/{self.version}/{self.advertise}/{recordType}/report',
                    json={
                      "reportDate": report_date,  #YYYYMMDD
                      "tactic": tc,
                      "metrics": ",".join(['campaignName','campaignId','adGroupName','adGroupId','impressions','clicks','cost','currency','attributedConversions1d','attributedConversions7d','attributedConversions14d','attributedConversions30d','attributedConversions1dSameSKU','attributedConversions7dSameSKU','attributedConversions14dSameSKU','attributedConversions30dSameSKU','attributedUnitsOrdered1d','attributedUnitsOrdered7d','attributedUnitsOrdered14d','attributedUnitsOrdered30d','attributedSales1d','attributedSales7d','attributedSales14d','attributedSales30d','attributedSales1dSameSKU','attributedSales7dSameSKU','attributedSales14dSameSKU','attributedSales30dSameSKU','attributedOrdersNewToBrand14d','attributedSalesNewToBrand14d','attributedUnitsOrderedNewToBrand14d'
                                           ]),
                    },
                    headers=self.headers,
                )
                #r.raise_for_status()
                r = r.json()
                self.logger.info(str(r))
                reportId = r["reportId"]

                while r['status'] == 'IN_PROGRESS':
                    r = requests.get(
                        f'{self.endpoint}/v1/reports/{reportId}',
                        headers=self.headers,
                    )
                    r = r.json()
                    print(r)
                self.logger.info('\n'+ str(r) +'\n')
                assert r['status'] == 'SUCCESS'

                location = r["location"]

                # actual download (s3) location returned in header here.
                # disable redirects
                r = requests.get(location, headers=self.headers, allow_redirects=False)
                location = r.headers["Location"]
                # download the report. do not send special headers as this is a regular
                # call to S3
                report = requests.get(location)
                report_content=gzip.decompress(report.content)
                report_content=json.loads(report_content)   
                report_content_final.append(report_content)

            results=[]
            for item in report_content_final:
                if type(item) is list and len(item) != 0:
                    for camp in item: 
                        results.append(camp)
            report_df= pd.DataFrame(results)
            report_df['campaignType'] = 'sponseredDisplay'
            report_df['extractionDate'] = report_date
            report_df.to_csv (file_path+'sponseredDisplay_adGroups_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredDisplay adGroup report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredDisplay adGroup report data")

    def report_product_ads(self):
        recordType = "productAds"
        tactic = ['T00020', 'T00030', 'remarketing']
        report_content_final=[]
        try:
            for tc in tactic:
                r = requests.post(
                    f'{self.endpoint}/{self.version}/{self.advertise}/{recordType}/report',
                    json={
                      "reportDate": report_date,  #YYYYMMDD
                      "tactic": tc,
                      "metrics": ",".join(['campaignName','campaignId','adGroupName','adGroupId','asin','sku','adId','impressions','clicks','cost','currency','attributedConversions1d','attributedConversions7d','attributedConversions14d','attributedConversions30d','attributedConversions1dSameSKU','attributedConversions7dSameSKU','attributedConversions14dSameSKU','attributedConversions30dSameSKU','attributedUnitsOrdered1d','attributedUnitsOrdered7d','attributedUnitsOrdered14d','attributedUnitsOrdered30d','attributedSales1d','attributedSales7d','attributedSales14d','attributedSales30d','attributedSales1dSameSKU','attributedSales7dSameSKU','attributedSales14dSameSKU','attributedSales30dSameSKU','attributedOrdersNewToBrand14d','attributedSalesNewToBrand14d','attributedUnitsOrderedNewToBrand14d'
                        ]),
                    },
                    headers=self.headers,
                )
                #r.raise_for_status()
                r = r.json()
                self.logger.info(str(r))
                reportId = r["reportId"]

                while r['status'] == 'IN_PROGRESS':
                    r = requests.get(
                        f'{self.endpoint}/v1/reports/{reportId}',
                        headers=self.headers,
                    )
                    r = r.json()
                    print(r)
                self.logger.info('\n'+ str(r) +'\n')
                assert r['status'] == 'SUCCESS'

                location = r["location"]
                # actual download (s3) location returned in header here.
                # disable redirects
                r = requests.get(location, headers=self.headers, allow_redirects=False)
                location = r.headers["Location"]
                # download the report. do not send special headers as this is a regular
                # call to S3
                report = requests.get(location)
                report_content=gzip.decompress(report.content)
                report_content=json.loads(report_content)   
                report_content_final.append(report_content)

            results=[]
            for item in report_content_final:
                if type(item) is list and len(item) != 0:
                    for camp in item: 
                        results.append(camp)
            report_df= pd.DataFrame(results)
            report_df['campaignType'] = 'sponseredDisplay'
            report_df['extractionDate'] = report_date
            report_df.to_csv (file_path+'sponseredDisplay_productAds_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredDisplay productAds report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredDisplay productAds report data")

    def report_targets(self):
        recordType = "targets"
        tactic = ['T00020', 'T00030']
        report_content_final=[]
        try:
            for tc in tactic:
                r = requests.post(
                    f'{self.endpoint}/{self.version}/{self.advertise}/{recordType}/report',
                    json={
                      "reportDate": report_date,  #YYYYMMDD
                      "tactic": tc,
                      "metrics": ",".join(['campaignName','campaignId','adGroupName','adGroupId','targetId','targetingExpression','targetingText','targetingType','impressions','clicks','cost','currency','attributedConversions1d','attributedConversions7d','attributedConversions14d','attributedConversions30d','attributedConversions1dSameSKU','attributedConversions7dSameSKU','attributedConversions14dSameSKU','attributedConversions30dSameSKU','attributedUnitsOrdered1d','attributedUnitsOrdered7d','attributedUnitsOrdered14d','attributedUnitsOrdered30d','attributedSales1d','attributedSales7d','attributedSales14d','attributedSales30d','attributedSales1dSameSKU','attributedSales7dSameSKU','attributedSales14dSameSKU','attributedSales30dSameSKU','attributedOrdersNewToBrand14d','attributedSalesNewToBrand14d','attributedUnitsOrderedNewToBrand14d'
                                            ]),
                    },
                    headers=self.headers,
                )
                #r.raise_for_status()
                r = r.json()
                reportId = r["reportId"]

                while r['status'] == 'IN_PROGRESS':
                    r = requests.get(
                        f'{self.endpoint}/v1/reports/{reportId}',
                        headers=self.headers,
                    )
                    r = r.json()
                    print(r)
                self.logger.info('\n'+ str(r) +'\n')
                assert r['status'] == 'SUCCESS'

                location = r["location"]
                # actual download (s3) location returned in header here.
                # disable redirects
                r = requests.get(location, headers=self.headers, allow_redirects=False)
                location = r.headers["Location"]

                # download the report. do not send special headers as this is a regular
                # call to S3
                report = requests.get(location)
                report_content=gzip.decompress(report.content)
                report_content=json.loads(report_content)
                report_content_final.append(report_content)

            results=[]
            for item in report_content_final:
                if type(item) is list and len(item) != 0:
                    for camp in item: 
                        results.append(camp)
            report_df= pd.DataFrame(results)
            report_df['campaignType'] = 'sponseredDisplay'
            report_df['extractionDate'] = report_date
            report_df.to_csv (file_path+'sponseredDisplay_targets_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredDisplay targets report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredDisplay targets report data")

Amz=AmazonSdReport()
Amz.report_camp()
Amz.report_camp_ad_group()
Amz.report_product_ads()
Amz.report_targets()

elapsed_time_secs=("script execution time :"+str(datetime.datetime.now()-start_time))
print("Program Executed in "+elapsed_time_secs) #It returns time in sec