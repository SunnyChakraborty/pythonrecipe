#!/usr/bin/env python3

"""

  Description: this script can be used to process each json workspace file and load into metastore --used postgresql here



"""


import my_login
import argparse
import sys, psycopg2
from datetime import datetime
from my.dataiapi.client import get_client
import json, os


## set default
MY_ADMIN_USER = my_login.admin_user
MY_ADMIN_PASS = my_login.admin_pass

print("MY_ADMIN_USER ={}".format(MY_ADMIN_USER))
print("MY_ADMIN_PASS ={}".format(MY_ADMIN_PASS))


class Pg:
  '''This is a docstring. Class containes all the API URLs'''
  def __init__(self,name):
     self.name=name

  def conn(self):
  '''This is a docstring. Class containes all the API URLs'''
      try:
         conn = psycopg2.connect(host="localhost",database="pbi_metadata", user=MY_ADMIN_USER, password=MY_ADMIN_PASS )
         conn.autocommit = True
         cur = conn.cursor()
         return cur
      except (Exception, psycopg2.DatabaseError) as e:
          print("Error is ",e)

def check_val(d,k):
  '''This is a docstring. Class containes all the API URLs'''
  if isinstance(d,dict) and k in d.keys():
     return d[k]
  else:
     return'N/A'


def new_conn():
  '''This is a docstring. Class containes all the API URLs'''
      try:
         conn = psycopg2.connect(host="localhost",database="pbi_metadata", user=MY_ADMIN_USER, password=MY_ADMIN_PASS )
         conn.autocommit = True
         cur = conn.cursor()
         return cur
      except (Exception, psycopg2.DatabaseError) as e:
          print("Error is ",e)

    
class Sql :
  '''This is a docstring. Class containes all the API URLs'''


   sql_check_workspace = "select 'EXISTS' from pbi_workspaces where id = '{0}'"
   sql_del1 = "delete from pbi_workspaces  where id = '{0}'"
   sql_del2 = "delete from pbi_datasources  where  workspaceId = '{0}'"
   sql_del3 = "delete from pbi_datasets  where workspaceId= '{0}'"
   sql_del4 = "delete from pbi_dsmodel_tables where workspaceId = '{0}'"
   sql_del5 = "delete from pbi_reports where workspaceId = '{0}'"
   sql_del6 = "delete from pbi_dashboards where workspaceId = '{0}'"
   sql_del7 = "delete from pbi_dashboard_tiles where workspaceId = '{0}'"
   sql_del8 = "delete from pbi_dcp_mapping where workspaceId = '{0}'"


   sql_pbi_workspaces = "insert INTO pbi_workspaces ( id, name, type, state) VALUES('{0}','{1}','{2}','{3}')"
   sql_pbi_datasources = "insert INTO pbi_datasources ( datasourceId, datasourceType, server, database,workspaceId) values ('{0}','{1}','{2}','{3}','{4}')"
   #sql_pbi_datasources = "insert INTO pbi_datasources ( datasourceId, datasourceType, server, database) values ('{0}','{1}','{2}','{3}')"
   sql_pbi_datasets = "insert into pbi_datasets ( id, name, configuredBy, datasourceInstanceId, workspaceId) values ('{0}','{1}','{2}','{3}','{4}')"
   sql_pbi_dsmodel_tables = "insert into pbi_dsmodel_tables ( id, name, description, datasetId,workspaceid) values (nextval('seq_pbi_dsmodel'),'{0}','{1}','{2}','{3}')"
   sql_pbi_reports = "insert into pbi_reports ( id, name, reporttype, createdDateTime, modifiedDateTime, modifiedBy ,datasetId, workspaceId , endorsement,certifiedby) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}', '{8}','{9}')"

   sql_pbi_dashboards = "insert into pbi_dashboards ( id, displayName, workspaceId) values ('{0}','{1}','{2}')"
   sql_pbi_dashboard_tiles = "insert into pbi_dashboard_tiles ( id, title, reportId, datasetId, dashboardId,workspaceId) values ('{0}','{1}','{2}','{3}','{4}','{5}')"
   #sql_pbi_dashboard_tiles = "insert into pbi_dashboard_tiles ( id, title, reportId, datasetId, dashboardId) values ('{0}','{1}','{2}','{3}','{4}')"
   sql_pbi_dcp_mapping = "INSERT INTO pbi_dcp_mapping (pbi_datasourceid,pbi_datasourcetype,pbi_server,pbi_database, workspaceid) VALUES('{0}','{1}','{2}','{3}','{4}')"

   sql_event_logger="INSERT INTO pbi_event_log (event_type,event_category,event_subcategory,event_name,event_log) values('{0}','{1}','{2}','{3}','{4}')"

class ScriptAbort(Exception):
    """used for aborting execution"""

def abort(err_msg, emit_fail=True):
    """abort script execution with an error and optionally print "FAILED"

    :param err_msg: the pre-formatted error message
    :param emit_fail: if True, print "FAILED" to stdout
    """
    if emit_fail is True:
        print ("FAILED")
    print("fatal error", file=sys.stderr)
    raise ScriptAbort


def validate_response(res, err_msg, is_abort=False):
  '''This is a docstring. Class containes all the API URLs'''
    if not res.success:
        if is_abort:
            abort(err_msg, emit_fail=False)
        print (err_msg)
        return False
    return True


def log(c,event_type,event_category,event_subcategory,event_name,event_log):
  '''This is a docstring. Class containes all the API URLs'''
    sql=Sql.sql_event_logger.format(event_type,event_category,event_subcategory,event_name,event_log)
    c.execute(sql)



def get_args():
    """Gets command-line args.

    :return: :py:class:`argparse.Namespace`
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--file-name', type=str, default="", help='Provide a valid json File name ')

    print(parser)
    return parser.parse_args()


def delete_workspace(workspace_id, c):
  '''This is a docstring. Class containes all the API URLs'''

     c.execute(Sql.sql_del1.format(workspace_id))
     c.execute(Sql.sql_del2.format(workspace_id))
     c.execute(Sql.sql_del3.format(workspace_id))
     c.execute(Sql.sql_del4.format(workspace_id))
     c.execute(Sql.sql_del5.format(workspace_id))
     c.execute(Sql.sql_del6.format(workspace_id))
     c.execute(Sql.sql_del7.format(workspace_id))
     c.execute(Sql.sql_del8.format(workspace_id))
     print("\n\n================== All data for workspace_id={} deleted ==================".format(workspace_id)) 

def check_workspace_exists(workspace_id, c):
  '''This is a docstring. Class containes all the API URLs'''

   sql_check=Sql.sql_check_workspace.format(workspace_id)
   print(sql_check)
   c.execute(sql_check)
   res= c.fetchall()
   print(res)
   print("length = {}".format(len(res)))
   if len(res) > 0:
     print("delete")
     delete_workspace(workspace_id, c)
     return (len(res))


def parse(pbi_payload_json) :
  '''This is a docstring. Class containes all the API URLs'''


  ##
  ##  load report to json dict
  ##  parse and insert into table 
  ##


  #log=Pg("logger")
  log_cur=new_conn()

  if pbi_payload_json :
      myjson=pbi_payload_json
  else:
      myjson='pbi_sample.json'

  file=open(myjson)
  #file=open('pbi_sample.json')
  f=json.load(file)


  wksp_name = ''
  d_datasets= []
  d_reports =[]


  try:
    conn = psycopg2.connect(host="localhost",database="pbi_metadata", user=MY_ADMIN_USER, password=MY_ADMIN_PASS )
    conn.autocommit = True
    cur = conn.cursor()

    if 'workspaces' in f:
      w=f['workspaces']
      if len(w) > 0:
        for x in w:
           print(x)
           print("\n\n\n\n======================================================\n\n")
           #Workspaces ==> Workspace_id, workspace_name
           workspace_id=x['id']

           ##################################################################################
           #### check if data exists for this workspace_id; if so ignore and exit unless overwite is given-in 
           #### which case delete all data for this workspace_id and reload
           ##################################################################################
           #check_workspace_exists(workspace_id, cur)


           wksp_name =x['name']
           wksp_sql = Sql.sql_pbi_workspaces.format(check_val(x,'id'),check_val(x,'name'),check_val(x,'type'),check_val(x,'state'))
           print(wksp_sql)
           cur.execute(wksp_sql)
           
           d_datasets= check_val(x,'datasets')
           d_reports = check_val(x,'reports')
           d_dashboards = check_val(x,'dashboards')

           for z1 in d_datasets:
             ds_name=z1['name']
             print("d_datasets====>\n{}".format(z1))
             if 'datasourceUsages' in z1.items():
               ds_datasourceUsages=z1['datasourceUsages']
               for z12 in ds_datasourceUsages :
                 ds_sql=Sql.sql_pbi_datasets.format(check_val(z1,'id'),check_val(z1,'name'),check_val(z1,'configuredBy'),check_val(z12,'datasourceInstanceId'),workspace_id) 
                 #ds_sql=Sql.sql_pbi_datasets.format(z1['id'],z1['name'],z1['configuredBy'],z1['datasourceUsages'][0]['datasourceInstanceId'],workspace_id) 
               print(ds_sql)
               cur.execute(ds_sql)
             else:
                 # log(c,event_type,event_category,event_subcategory,event_name,event_log)
                 log(log_cur,'INFO','API','INPUT','datasourceUsages','TAG is missing: {0} for WORKSPACE: {1}'.format('datasourceUsages',workspace_id))
                 print("\n............tag is missing ==>  'datasourceUsages' ..................")


             if 'tables' in z1.items():
               for z11 in z1['tables'] :
                  table_name=check_val(z11,'name') 
                  model_sql=Sql.sql_pbi_dsmodel_tables.format(check_val(z11,'name'),check_val(z11,'description'),check_val(z1,'id'),workspace_id)
                  print(model_sql)
                  cur.execute(model_sql)
             else:
                 log(log_cur,'INFO','API','INPUT','datasourceUsages','TAG is missing: {0} for WORKSPACE: {1}'.format('tables',workspace_id))
                 print("\n............tag is missing ==>  'tables' ..................")

           # check reports
           print('d_reports===>{}'.format(d_reports))
           for z2 in d_reports :
             endorsementDetails=check_val(z2,'endorsementDetails')
             if endorsementDetails == 'N/A':
                log(log_cur,'INFO','API','INPUT','endorsementDetails','TAG is missing: {0} for WORKSPACE: {1}'.format('endorsementDetails',workspace_id)) 

             rpt_sql=Sql.sql_pbi_reports.format(check_val(z2,'id'),check_val(z2,'name'),check_val(z2,'reportType'),check_val(z2,'createdDateTime'),check_val(z2,'modifiedDateTime'),check_val(z2,'modifiedBy'),check_val(z2,'datasetId'),workspace_id,check_val(z2['endorsementDetails'],'endorsement') if endorsementDetails !='N/A' else 'N/A',check_val(z2['endorsementDetails'],'certifiedBy') if endorsementDetails !='N/A' else 'N/A')


             #rpt_sql=Sql.sql_pbi_reports.format(z2['id'],z2['name'],z2['reportType'],z2['createdDateTime'],z2['modifiedDateTime'],z2['modifiedBy'],z2['datasetId'],workspace_id,z2['endorsementDetails']['endorsement'],z2['endorsementDetails']['certifiedBy'])
             print(rpt_sql)
             cur.execute(rpt_sql)


           # check dashboards
           for z2 in d_dashboards :
             dashboardId = check_val(z2,'id')
             dashboards_sql=Sql.sql_pbi_dashboards .format(check_val(z2,'id'),check_val(z2,'displayName'),workspace_id)
             print(dashboards_sql)
             cur.execute(dashboards_sql)
             
             d_tiles = check_val(z2,'tiles')
             for z22 in d_tiles:
                tiles_sql=Sql.sql_pbi_dashboard_tiles.format(check_val(z22,'id'),check_val(z22,'title'),check_val(z22,'reportId'),check_val(z22,'datasetId'),dashboardId,workspace_id)
                print(tiles_sql)
                cur.execute(tiles_sql)
       

    # Data Source details => name, server, database,Workspace_id
    if 'datasourceInstances' in f:
      s=f['datasourceInstances']
      for x1 in s:
         datasourceType = check_val(x1,'datasourceType')
         if 'connectionDetails' in x1:
             connectionDetails = check_val(x1,'connectionDetails')
             server = check_val(connectionDetails,'server')
             db_name=check_val(connectionDetails,'database')

             datasourceId=check_val(x1,'datasourceId')
             datasrc_sql=Sql.sql_pbi_datasources.format(datasourceId,datasourceType,server,db_name,workspace_id)

             cur.execute(datasrc_sql)
             print("Created data_source_details. \n")
             ## adding to mapping table
             #(pbi_datasourceid,pbi_datasourcetype,pbi_server,pbi_database, workspaceid)
             mapping_sql=Sql.sql_pbi_dcp_mapping.format(datasourceId,datasourceType,server,db_name,workspace_id)
             cur.execute(mapping_sql) 
  except (Exception, psycopg2.DatabaseError) as e:
     print("Error is ",e)
  finally:
     if conn is not None:
      conn.close()
     print('Database connection closed.')






def main():
  '''This is a docstring. Class containes all the API URLs'''
  args = get_args()

  payload_file_name=args.file_name
  print("Input file ==> {}".format(payload_file_name))
  if not payload_file_name :
     print ("Must provide input parameter json payload file name ..exiting..\n")
     exit(0)
  parse(payload_file_name)


if __name__=="__main__" :
  '''This is a docstring. Class containes all the API URLs'''
    main()

