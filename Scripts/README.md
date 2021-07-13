**Requirements For Amazon-Ads-API Code**


1.Python 3.7<br />

**About Code**

To access Amazon Ads API A developer needs to create profile on amazon developer portal https://developer.amazon.com/apps-and-games/console/apps/list.html? 
After that create an app and generate client id, client secret. Make sure you have access to Required seller account. Using these params generate a refresh token (one time only). Steps to generate refresh token can be found on https://advertising.amazon.com/API/docs/en-us/get-started/generate-api-tokens .
I have created 8 scripts here. There are three categories present in AMS data (sponsored product, brands, display). Script named sponsoredProducts_<> can fetch both sponsored product and brands data and data can be pull from version v1 of API. Script named sponsoredDisplay_<> can fetch both sponsored display data and data can be pull from version v2 of API. Both of these scripts were created separately to have all attribute data like (campaign, productAds, AdGroups etc) and reports data as well.
Also, I have added attribution, portfolio and amazon dsp script as well to fetch attribution and DSP data. Script DSP is under development.<br />



**Installation Steps**


1.On Terminal clone the Repository "git clone git@github.com:JNipun/Amazon-Ads-API.git"<br />
2.Go to downloaded folder<br />
3.pip install -r requirements.txt<br />



**Execution:-**


```
python <script_name>.py 

```

```
In case of any error in any of this script, please go to in Logs folder and check the detailed logs. After successful run output will be generated in csv format under Data folder.

```


**Amazon Ads API Cred Config(Location:resources/Amz_Seller_Cred.json)**
```
{
  "client_id" : "",
  "client_secret" : "",
  "profileId" : "",
  "endpoint" : "",
  "version" : "",
  "refresh_token" : ""

}
```
Add credentials here and make sure you have access of all the params.