#!/usr/bin/env python3

"""

  Description: Script to parse complex nested json and flatten it to csv file.



"""

import json
import csv
import argparse
import sys, os

CURRENT_DIR=os.getcwd()
INPUT_FILE=''
OUTPUT_CSV_FILE='{0}/{1}'.format(CURRENT_DIR,'output.csv')

def get_args():
    """Gets command-line args.

    :return: :py:class:`argparse.Namespace`
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-json-file', type=str, default=INPUT_FILE, help='Provide a valid json file as expected')
    parser.add_argument('--output-csv-file', type=str, default=OUTPUT_CSV_FILE, help='Provide a output csv file name')

    print(parser)
    return parser.parse_args()



def process():
  '''This is a docstring. function is to process '''

  with open(INPUT_FILE,encoding='utf-8-sig') as fh:
     d=json.load(fh)

  header_list=['GUID','GENRTD_TMSTMP','NAME','QUESTION_NUMBER','QUESTION_CHOICE','QUESTION_TEXT','ANSWER']
  build_rows_list=[]
  build_rows_list.append(header_list)
#  csv_list.append(['GUID','GENRTD_TMSTMP','NAME','QUESTION_NUMBER','QUESTION_CHOICE','QUESTION_TEXT','ANSWER'])


  GUID=d["summary"]['projectGuid']
  GENRTD_TMSTMP=d["summary"]['generated']
  for x in d["summary"]['pages']:
    #get accountid
    if x['name']=='General Information':
       NAME=x['name']
       for x1 in x['layout'] :
          print(x1['question']['text'],'-',x1['question']['value'],'-',x1['question']['id'])
          value=x1['question']['value']
          if isinstance(value,dict) and 'value' in value.keys():
             pick_value=value['value']
          elif isinstance(value,str):
             pick_value=value
          else:
             pick_value=''
          build_rows_list.append([GUID,GENRTD_TMSTMP,NAME,x1['question']['text'],pick_value,'{0}'.format(x1['question']['id'])])
          #build_rows_list.append([GUID,GENRTD_TMSTMP,NAME,x1['question']['text'],x1['question']['value'],'{0}'.format(x1['question']['id'])])
  
  
    # get contacts
    if x['name']=='Contacts':
       NAME=x['name']
       for x1 in x['layout'] :
        x2=x1['repeatingSection']
        if 'rows' in x2.keys():
          x3=x2['rows']
          for x4 in x2['rows']:
             for x5 in x4['layout']:
               x6=x5['fragmentPortion']['layout']
               #print(x6)
               for x7 in x6:
                 #print(x7)
                 if 'question' in x7.keys():
                  print(x7['question']['text'],'-',x7['question']['value'],'-',x7['question']['id'])
                  value=x7['question']['value']
                  if isinstance(value,dict) and 'value' in value.keys():
                     pick_value=value['value']
                  elif isinstance(value,str):
                     pick_value=value
                  else:
                     pick_value=''
                  build_rows_list.append([GUID,GENRTD_TMSTMP,NAME,x7['question']['text'],pick_value,'{0}'.format(x7['question']['id'])])
                 elif 'fragmentPortion' in x7.keys():
                   x8=x7['fragmentPortion']['layout']
                   for x9 in x8:
                      #print(x9) 
                      value=x9['question']['value']
                      if isinstance(value,dict) and 'value' in value.keys():
                        pick_value=value['value']
                      elif isinstance(value,str):
                        pick_value=value
                      else:
                        pick_value=''
                      print(x9['question']['text'],'-',x9['question']['value'],'-',x9['question']['id'])
                      build_rows_list.append([GUID,GENRTD_TMSTMP,NAME,x9['question']['text'],pick_value,'{0}'.format(x9['question']['id'])])
  
    # get Communications & Notices
    if x['name']=='Communications & Notices':
       NAME=x['name']
       for x1 in x['layout'] :
          value=x1['question']['value']
          if isinstance(value,dict) and 'value' in value.keys():
            pick_value=value['value']
          elif isinstance(value,str):
            pick_value=value
          else:
            pick_value=''
          print(x1['question']['text'],'-',x1['question']['value'],'-',x1['question']['id'])
          build_rows_list.append([GUID,GENRTD_TMSTMP,NAME,x1['question']['text'],x1['question']['value'],'{0}'.format(x1['question']['id'])])
  
  
  print( build_rows_list)
  f=open(OUTPUT_CSV_FILE,"w")
  #f=open("output_file.csv","w")
  writer=csv.writer(f)
  for row in build_rows_list:
     #print(row)
     #row=",".join(x)
     writer.writerow(row)
  
  f.close()
  print("..............Output File: {}...................".format(OUTPUT_CSV_FILE))
  print(".............................Done ............................") 



def main():
  '''This is a docstring. function is to main '''
    print("processing main...")
    args = get_args()
    o_input_json=args.input_json_file
    o_out_csv_file=args.output_csv_file
    if (not o_input_json):
       print('Provide a valid inpiut JSON file')
       exit(-1)
    global INPUT_FILE
    global OUTPUT_CSV_FILE
    INPUT_FILE=o_input_json
    if (not o_out_csv_file):
       print('Output will be written to: {}'.format(OUTPUT_CSV_FILE))
    else:
      OUTPUT_CSV_FILE='{0}/{1}'.format(CURRENT_DIR,o_out_csv_file)

    process()

if __name__ == '__main__':
    sys.exit(main())

