#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import json
import gzip
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
report_date=(datetime.datetime.now()- datetime.timedelta(1)).strftime("%Y%m%d")

try:
    if os.path.exists(file_path):
        pass
    else:
        os.mkdir(file_path)
except OSError:
    print ("Creation of the directory %s failed" % path)


class AmazonSpReport:
    def __init__(self):
        self.logger = log_handler(__name__ + '.' + self.__class__.__name__)
        try:
                with open('resources/Amz_Seller_Cred.json', 'r') as f:
                    self.logger.info('*************************')
                    self.logger.info("sponseredProducts and brands Reports execution has been strated at :"+str(cur_date) +"\n")
                    cred = load(f)
                    self.client_id = cred['client_id']
                    self.client_secret = cred['client_secret']
                    self.profileId = cred['profileId']
                    self.endpoint = 'https://'+ str(cred['endpoint'])
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


    def report_camp(self):
        recordType = "campaigns"
        campaignType =  ["sponsoredProducts","sponsoredBrands","headlineSearch"]
        report_content_final=[]
        try:
            for ct in campaignType:
                r = requests.post(
                    f'{self.endpoint}/{self.version}/{recordType}/report',
                    json={
                      "campaignType": ct,
                      "reportDate": report_date,  #YYYYMMDD
                      "metrics": ",".join(["campaignName","campaignId","campaignStatus","campaignType","campaignBudget","campaignRuleBasedBudget","applicableBudgetRuleId","applicableBudgetRuleName",
                        "impressions","clicks","cost","attributedConversions1d","attributedConversions7d","attributedConversions14d","attributedConversions30d",
                        "attributedConversions1dSameSKU","attributedConversions7dSameSKU","attributedConversions14dSameSKU","attributedConversions30dSameSKU","attributedUnitsOrdered1d",
                        "attributedUnitsOrdered7d","attributedUnitsOrdered14d","attributedUnitsOrdered30d","attributedSales1d","attributedSales7d","attributedSales14d",
                        "attributedSales30d","attributedSales1dSameSKU","attributedSales7dSameSKU","attributedSales14dSameSKU","attributedSales30dSameSKU",
                        "attributedUnitsOrdered1dSameSKU","attributedUnitsOrdered7dSameSKU","attributedUnitsOrdered14dSameSKU","attributedUnitsOrdered30dSameSKU"
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
                        f'{self.endpoint}/{self.version}/reports/{reportId}',
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
            for value in report_content_final:
                for var in value: 
                    results.append(var)
            report_df= pd.DataFrame(results)
            report_df['extractionDate'] = report_date
            report_df.to_csv(file_path+'sponsoredProducts_campaign_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredProducts campaign report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredProducts campaign report data")

    def report_camp_ad_group(self):
        recordType = "adGroups"
        campaignType = ["sponsoredProducts","sponsoredBrands","headlineSearch"]
        report_content_final=[]
        try:
            for ct in campaignType:
                r = requests.post(
                    f'{self.endpoint}/{self.version}/{recordType}/report',
                    json={
                      "campaignType": ct,
                      "reportDate": report_date,  #YYYYMMDD
                      "metrics": ",".join(["campaignName","campaignId","campaignStatus","campaignType","adGroupName","adGroupId","impressions","clicks","cost","attributedConversions1d","attributedConversions7d",
                        "attributedConversions14d","attributedConversions30d","attributedConversions1dSameSKU","attributedConversions7dSameSKU","attributedConversions14dSameSKU",
                        "attributedConversions30dSameSKU","attributedUnitsOrdered1d","attributedUnitsOrdered7d","attributedUnitsOrdered14d","attributedUnitsOrdered30d",
                        "attributedSales1d","attributedSales7d","attributedSales14d","attributedSales30d","attributedSales1dSameSKU","attributedSales7dSameSKU",
                        "attributedSales14dSameSKU","attributedSales30dSameSKU","attributedUnitsOrdered1dSameSKU","attributedUnitsOrdered7dSameSKU","attributedUnitsOrdered14dSameSKU",
                        "attributedUnitsOrdered30dSameSKU"]),
                    },
                    headers=self.headers,
                )
                #r.raise_for_status()
                r = r.json()
                self.logger.info(str(r))  
                reportId = r["reportId"]

                while r['status'] == 'IN_PROGRESS':
                    r = requests.get(
                        f'{self.endpoint}/{self.version}/reports/{reportId}',
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
            for value in report_content_final:
                for var in value: 
                    results.append(var)
            report_df= pd.DataFrame(results)
            report_df['extractionDate'] = report_date
            report_df.to_csv(file_path+'sponsoredProducts_adGroups_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredProducts adGroup report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredProducts adGroup report data")

    def report_keywords_group(self):
        recordType = "keywords"
        campaignType = ["sponsoredProducts","sponsoredBrands","headlineSearch"]
        report_content_final=[]
        try:
            for ct in campaignType:
                r = requests.post(
                    f'{self.endpoint}/{self.version}/{recordType}/report',
                    json={
                      "campaignType": ct,
                      "reportDate": report_date,  #YYYYMMDD
                      "metrics": ",".join(['campaignName', 'campaignId',"campaignStatus","campaignType", 'adGroupName', 'adGroupId', 'keywordId', 'keywordText', 'matchType', 'impressions', 'clicks', 'cost', 
                        'attributedConversions1d', 'attributedConversions7d', 'attributedConversions14d', 'attributedConversions30d', 'attributedConversions1dSameSKU', 
                        'attributedConversions7dSameSKU', 'attributedConversions14dSameSKU', 'attributedConversions30dSameSKU', 'attributedUnitsOrdered1d', 'attributedUnitsOrdered7d', 
                        'attributedUnitsOrdered14d', 'attributedUnitsOrdered30d', 'attributedSales1d', 'attributedSales7d', 'attributedSales14d', 'attributedSales30d', 
                        'attributedSales1dSameSKU', 'attributedSales7dSameSKU', 'attributedSales14dSameSKU', 'attributedSales30dSameSKU', 'attributedUnitsOrdered1dSameSKU', 
                        'attributedUnitsOrdered7dSameSKU', 'attributedUnitsOrdered14dSameSKU', 'attributedUnitsOrdered30dSameSKU'
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
                        f'{self.endpoint}/{self.version}/reports/{reportId}',
                        headers=self.headers,
                    )
                    r = r.json()
                    print(r)
                self.logger.info('\n'+ str(r) +'\n')  
                assert r['status'] == 'SUCCESS'

                location = r["location"]
                #print(location)

                # actual download (s3) location returned in header here.
                # disable redirects
                r = requests.get(location, headers=self.headers, allow_redirects=False)
                location = r.headers["Location"]
                #print(location)


                # download the report. do not send special headers as this is a regular
                # call to S3
                report = requests.get(location)
                report_content=gzip.decompress(report.content)
                report_content=json.loads(report_content)
                report_content_final.append(report_content)
            results=[]
            for value in report_content_final:
                for var in value: 
                    results.append(var)
            report_df= pd.DataFrame(results)
            report_df['extractionDate'] = report_date
            report_df.to_csv(file_path+'sponsoredProducts_keywrods_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredProducts keywords report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredProducts keywords report data")

    def report_product_ads(self):
        recordType = "productAds"
        try:
            r = requests.post(
                f'{self.endpoint}/{self.version}/{recordType}/report',
                json={
                  "campaignType": "sponsoredProducts",
                  "reportDate": report_date,  #YYYYMMDD
                  "metrics": ",".join(['campaignName', 'campaignId',"campaignStatus","campaignType", 'adGroupName', 'adGroupId', 'impressions', 'clicks', 'cost', 'currency','asin', 'attributedConversions1d', 
                    'attributedConversions7d', 'attributedConversions14d', 'attributedConversions30d', 'attributedConversions1dSameSKU', 'attributedConversions7dSameSKU', 
                    'attributedConversions14dSameSKU', 'attributedConversions30dSameSKU', 'attributedUnitsOrdered1d', 'attributedUnitsOrdered7d', 'attributedUnitsOrdered14d',
                     'attributedUnitsOrdered30d', 'attributedSales1d', 'attributedSales7d', 'attributedSales14d', 'attributedSales30d', 'attributedSales1dSameSKU', 
                     'attributedSales7dSameSKU', 'attributedSales14dSameSKU', 'attributedSales30dSameSKU', 'attributedUnitsOrdered1dSameSKU', 'attributedUnitsOrdered7dSameSKU', 'attributedUnitsOrdered14dSameSKU', 'attributedUnitsOrdered30dSameSKU'
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
                    f'{self.endpoint}/{self.version}/reports/{reportId}',
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
            #print(location)


            # download the report. do not send special headers as this is a regular
            # call to S3
            report = requests.get(location)
            report_content=gzip.decompress(report.content)
            report_content=json.loads(report_content)
            results=[]
            for value in report_content:
                new_dict={}
                for productAds_key,productAds_value in value.items(): 
                    new_dict[productAds_key]=productAds_value
                results.append(new_dict)
            report_df= pd.DataFrame(results)
            report_df['extractionDate'] = report_date
            report_df.to_csv(file_path+'sponsoredProducts_productAds_report_'+report_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.exception("sponsoredProducts productAds report has been generated successfully")
        except Exception as e:
            self.logger.exception("Error in generating sponsoredProducts productAds report data")

Amz=AmazonSpReport()
Amz.report_camp()
Amz.report_camp_ad_group()
Amz.report_keywords_group()
Amz.report_product_ads()

elapsed_time_secs=("script execution time :"+str(datetime.datetime.now()-start_time))
print(elapsed_time_secs) #It returns time in sec