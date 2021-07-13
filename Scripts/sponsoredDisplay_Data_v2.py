#!/usr/bin/python
# -*- coding: utf-8 -*-

from io import BytesIO
import urllib.request
import urllib.parse
import gzip
import json
import requests
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

try:
    if os.path.exists(file_path):
        pass
    else:
        os.mkdir(file_path)
except OSError:
    print ("Creation of the directory %s failed" % path)


class AdvertisingApi:
    def __init__(self):
        self.logger = log_handler(__name__ + '.' + self.__class__.__name__)
        try:
                with open('resources/Amz_Seller_Cred.json', 'r') as f:
                    self.logger.info('*************************')
                    self.logger.info("sponseredDisplay Data fetching execution has been strated at :"+str(cur_date) +"\n")
                    cred = load(f)
                    self.client_id = cred['client_id']
                    self.client_secret = cred['client_secret']
                    self.profileId = cred['profileId']
                    self.endpoint = cred['endpoint']
                    self.profileId = cred['profileId']
                    self.api_version = 'v2'
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

    def _operation(self,interface, params=None, method='GET'):
        """
        Makes that actual API call.

        :param interface: Interface used for this call.
        :type interface: string
        :param params: Parameters associated with this call.
        :type params: GET: string POST: dictionary
        :param method: Call method. Should be either 'GET' or 'POST'
        :type method: string
        """
        if self.access_token is None:
            return {'success': False,
                    'code': 0,
                    'response': 'access_token is empty.'}

        headers = {'Authorization': 'bearer {}'.format(self.access_token),
                   'Content-Type': 'application/json'}

        if self.profileId is not None and self.profileId != '':
            headers['Amazon-Advertising-API-Scope'] = self.profileId

        data = None

        if method == 'GET':
            if params is not None:
                p = '?{}'.format(urllib.parse.urlencode(params))
            else:
                p = ''

            url = 'https://{host}/{api_version}/{interface}{params}'.format(
                host=self.endpoint,
                api_version=self.api_version,
                interface=interface,
                params=p)
        else:
            if params is not None:
                data = json.dumps(params).encode('utf-8')

            url = 'https://{host}/{api_version}/{interface}'.format(
                host=self.endpoint,
                api_version=self.api_version,
                interface=self.interface)

        req = urllib.request.Request(url=url, headers=headers, data=data)
        req.method = method

        try:
            f = urllib.request.urlopen(req)
            return {'success': True,
                        'code': f.code,
                        'response': f.read().decode('utf-8')}
        except urllib.error.HTTPError as e:
            return {'success': False,
                    'code': e.code,
                    'response': e.msg}

    def get_profiles(self):
        interface = 'profiles'
        print(self._operation(interface))

    def list_campaigns(self):
        try:
            interface = 'sd/campaigns'
            campaigns_raw=self._operation(interface)
            campaigns=campaigns_raw.get('response')
            campaigns=json.loads(campaigns)
            results=[]
            for item in campaigns:
                new_dict={}
                data = dict(item)
                for camp_key,camp_value in data.items(): 
                    if type(camp_value) is dict:
                        for camp_bidding_key, camp_bidding_value in camp_value.items():
                            if type(camp_bidding_value) is list:
                                for camp_bidding_adjustments_value in camp_bidding_value:
                                    if type(camp_bidding_adjustments_value) is dict:
                                        for camp_bidding_adjustments_p_key,camp_bidding_adjustments_p_value in camp_bidding_adjustments_value.items():  
                                            new_dict[camp_key+"_"+camp_bidding_key+"_"+camp_bidding_adjustments_p_key]=camp_bidding_adjustments_p_value  
                                    else:        
                                        new_dict[camp_key+"_"+camp_bidding_key]=sub_value_next                   
                            else:    
                                new_dict[camp_key+"_"+camp_bidding_key]=camp_bidding_value
                    else:
                        new_dict[camp_key]=camp_value
                results.append(new_dict)
            campaign_df= pd.DataFrame(results)
            campaign_df.to_csv (file_path+'sponseredDisplay_campaigns.csv',encoding="utf-8-sig",index=False)
            self.logger.info('sponseredDisplay campaigns data have been fetched successfully')
            
        except Exception as e:
                self.logger.exception("Error in fetching of sponseredDisplay campaigns data")


    def list_campaigns_ex(self):
        try:
            interface = 'sd/campaigns/extended'
            campaigns_raw=self._operation(interface)
            campaigns=campaigns_raw.get('response')
            campaigns=json.loads(campaigns)
            results=[]
            for item in campaigns:
                new_dict={}
                data = dict(item)
                for camp_key,camp_value in data.items(): 
                    if type(camp_value) is dict:
                        for camp_bidding_key, camp_bidding_value in camp_value.items():
                            if type(camp_bidding_value) is list:
                                for camp_bidding_adjustments_value in camp_bidding_value:
                                    if type(camp_bidding_adjustments_value) is dict:
                                        for camp_bidding_adjustments_p_key,camp_bidding_adjustments_p_value in camp_bidding_adjustments_value.items():  
                                            new_dict[camp_key+"_"+camp_bidding_key+"_"+camp_bidding_adjustments_p_key]=camp_bidding_adjustments_p_value  
                                    else:        
                                        new_dict[camp_key+"_"+camp_bidding_key]=sub_value_next                   
                            else:    
                                new_dict[camp_key+"_"+camp_bidding_key]=camp_bidding_value
                    else:
                        new_dict[camp_key]=camp_value
                results.append(new_dict)
            campaign_ex_df= pd.DataFrame(results)
            campaign_ex_df.to_csv (file_path+'sponseredDisplay_campaigns_extended.csv',encoding="utf-8-sig",index=False)
            self.logger.info('sponseredDisplay campaigns extended data have been fetched successfully')
            
        except Exception as e:
                self.logger.exception("Error in fetching of sponseredDisplay campaigns extended data")

    def list_ad_groups(self):
        try:
            interface = 'sd/adGroups'
            campaigns_ad_grp_raw=self._operation(interface)
            campaigns_ad_grp=campaigns_ad_grp_raw.get('response')
            campaigns_ad_grp=json.loads(campaigns_ad_grp)
            results=[]
            #print(campaigns)
            for item in campaigns_ad_grp:
                new_dict={}
                data = dict(item)
                for camp_ad_key,camp_ad_value in data.items(): 
                    
                    new_dict[camp_ad_key]=camp_ad_value
                results.append(new_dict)
            campaign_ad_grp_df= pd.DataFrame(results)
            campaign_ad_grp_df.to_csv (file_path+'sponseredDisplay_adGroups.csv')
            self.logger.info('sponseredDisplay ad groups data have been fetched successfully')
            
        except Exception as e:
                self.logger.exception("Error in fetching of sponseredDisplay adGroups data")

    def list_ad_groups_ex(self):
        try:
            interface = 'sd/adGroups/extended'
            campaigns_ad_grp_raw=self._operation(interface)
            campaigns_ad_grp_ex=campaigns_ad_grp_raw.get('response')
            campaigns_ad_grp_ex=json.loads(campaigns_ad_grp_ex)
            results=[]
            for item in campaigns_ad_grp_ex:
                new_dict={}
                data = dict(item)
                for camp_ad_key,camp_ad_value in data.items(): 
                    
                    new_dict[camp_ad_key]=camp_ad_value
                results.append(new_dict)
            campaign_ad_grp_ex_df= pd.DataFrame(results)
            campaign_ad_grp_ex_df.to_csv (file_path+'sponseredDisplay_adGroups_extended.csv',encoding="utf-8-sig",index=False)
            self.logger.info('sponseredDisplay adGroups extended data have been fetched successfully')
            
        except Exception as e:
                self.logger.exception("Error in fetching of sponseredDisplay adGroups extended data")

    def list_product_ads(self):
        try:
            interface = 'sd/productAds'
            product_ad_raw=self._operation(interface)
            product_ads=product_ad_raw.get('response')
            product_ads=json.loads(product_ads)
            results=[]
            for item in product_ads:
                new_dict={}
                data = dict(item)
                for prod_ad_key,prod_ad_value in data.items(): 
                    
                    new_dict[prod_ad_key]=prod_ad_value
                results.append(new_dict)
            product_ads_df= pd.DataFrame(results)
            product_ads_df.to_csv (file_path+'sponseredDisplay_productAds.csv',encoding="utf-8-sig",index=False)
            self.logger.info('sponseredDisplay Product data have been fetched successfully')
        except Exception as e:
                self.logger.exception("Error in fetching of sponseredDisplay products data")

    def list_product_ads_ex(self):
        try:
            interface = 'sd/productAds/extended'
            product_ad_ex_raw=self._operation(interface)
            product_ads_ex=product_ad_ex_raw.get('response')
            product_ads_ex=json.loads(product_ads_ex)
            results=[]
            for item in product_ads_ex:
                new_dict={}
                data = dict(item)
                for prod_ad_key,prod_ad_value in data.items(): 
                    
                    new_dict[prod_ad_key]=prod_ad_value
                results.append(new_dict)
            product_ads_ex_df= pd.DataFrame(results)
            product_ads_ex_df.to_csv (file_path+'sponseredDisplay_productAds_extended.csv',encoding="utf-8-sig",index=False)
            self.logger.info('sponseredDisplay Product extended data have been fetched successfully\n')
            self.logger.info('*************************\n')
        except Exception as e:
            self.logger.exception("Error in fetching of sponseredDisplay products extended data \n")

Amz=AdvertisingApi()
#Amz.get_profiles()
Amz.list_campaigns()
Amz.list_campaigns_ex()
Amz.list_ad_groups()
Amz.list_ad_groups_ex()
Amz.list_product_ads()
Amz.list_product_ads_ex()

elapsed_time_secs=("script execution time :"+str(datetime.datetime.now()-start_time))
print(elapsed_time_secs) #It returns time in sec