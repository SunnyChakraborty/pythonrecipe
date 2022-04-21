#!/usr/bin/env python3

"""

  Description: this script can be used to pull workspace payload from powerBI accounts as json format and each workspace is written to a payload/ dir
  as json file with timestamp



"""

import requests
import json
from datetime import datetime
import time

TOKEN=""

class ApiUrl:
   '''This is a docstring. Class containes all the API URLs'''
   ##get token
   url_token="""
              https://login.microsoftonline.com/b2a1230f-7e46-4cdd-bebf-30f82640ea8c/oauth2/token
   """


   url_get_wksp="""
     https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?modifiedSince=2022-03-04T05:51:30.0000000Z&excludePersonalWorkspaces=True
   """

   url_scan_status="""
      https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{0}
   """


   url_scan_results="""
      https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{0}
   """


   url_metadata="""
       https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True&getArtifactUsers=True
   """


def get_token():

   '''This is a docstring. Class containes all the API URLs'''
   url=ApiUrl.url_token
   grant_type="client_credentials"
   resource="https://analysis.windows.net/powerbi/api"
   client_id="d975a846-....................."
   client_secret="C6h7Q~...................."

   #sample output

   output={
    "token_type": "Bearer",
    "expires_in": "3599",
    "ext_expires_in": "3599",
    "expires_on": "1649397685",
    "not_before": "1649393785",
    "resource": "https://analysis.windows.net/powerbi/api",
    "access_token": "eyJ0eX.................."  
}


   url = "https://login.microsoftonline.com/b2a1230f-7e46-4cdd-bennnnnnnnnnnc/oauth2/token"

   payload = "grant_type=client_credentials\r\n&resource={0}\r\n&client_id={1}\r\n&client_secret={2}\r\n".format(resource,client_id,client_secret)
   headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'fpc=Arh6wLXL9KhNv2pHEeTHQD46pC-sAQAAAHC44dkOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
      }

   response = requests.request("POST", url, headers=headers, data=payload)

   #print(response.text,"\n\n")
   token_dict=json.loads(response.text)
   token=token_dict["access_token"]
   #print("....................\n\n{}\n\n................".format(token))
   global TOKEN
   TOKEN=token
   return (token)


def get_list_of_workspaces(modified_since):
   '''This is a docstring. Class containes all the API URLs'''


   url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?modifiedSince={0}&excludePersonalWorkspaces=True".format(modified_since)
   token=get_token()

   payload={}
   headers = {
     'Authorization': 'Bearer {0}'.format(token)
   }

   response = requests.request("GET", url, headers=headers, data=payload)

   #print(response.text)
   j=json.loads(response.text)
   return(j)


def get_metadata(workspace_id):
   '''This is a docstring. Class containes all the API URLs'''

    url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True&getArtifactUsers=True"

    payload = json.dumps({
      "workspaces": [
        "{0}".format(workspace_id)
      ]
    })


    headers = {
     'Authorization': 'Bearer {0}'.format(TOKEN),
     'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
    j=json.loads(response.text)
    return(j)

def get_scan_status(scan_id):
   '''This is a docstring. Class containes all the API URLs'''

   url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{0}".format(scan_id)

   payload = ""
   headers = {
     'Authorization': 'Bearer {0}'.format(TOKEN)
   }

   response = requests.request("GET", url, headers=headers, data=payload)

   j=json.loads(response.text)
   return(j)

def fetch_scan_result(scan_id):
   '''This is a docstring. Class containes all the API URLs'''

  url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{0}".format(scan_id)

  payload = ""
  headers = {
  'Authorization': 'Bearer {0}'.format(TOKEN)
  }

  response = requests.request("GET", url, headers=headers, data=payload)

  #print(response.text)
  j=json.loads(response.text)
  return(j)


def main():
   '''This is a docstring. Class containes all the API URLs'''

  token=get_token()
  print("token: {}".format(token))
  list_of_payload=[]

  #test data--this must be passed in for incremental
  modified_since="2022-03-25T05:51:30.0000000Z"
  workspaces_dict=get_list_of_workspaces(modified_since)
  print(workspaces_dict)
  if len(workspaces_dict) >0 :
   for wk in workspaces_dict:
      workspace_id=wk['id']
      print(wk['id'])
      res=get_metadata(workspace_id)
      print(res["id"])
      if (isinstance(res,dict) and res["id"]) :
          #scan=get_scan_status(res["id"])
          while (get_scan_status(res["id"])['status'] !='Succeeded'):
               time.sleep(15)
          scan=get_scan_status(res["id"])
          #print(scan)
          #{'id': '3d6d9435-bc27-4066-afdb-af3d5a2f4672', 'createdDateTime': '2022-04-11T15:43:04.95', 'status': 'Succeeded'}

          if (isinstance(scan,dict) and scan["id"] and scan['status'] =='Succeeded') :
            print('fetch scan result')
            print('\n--------------------------------[scan result for WK={0}]-----------------------------------'.format(workspace_id))
            scan_result=fetch_scan_result(scan["id"])
            #print(scan_result)
            payload='pbi-payload-'+datetime.now().strftime("%b-%d-%Y-%H-%M-%f")+'.json'
            list_of_payload.append(payload)
            with open('payload/'+payload,'w', encoding='utf-8') as f:
              json.dump(scan_result,f)




  print("----------------------------------Done---------------------------\n")
  print(list_of_payload)




if __name__=="__main__" :
    main()

