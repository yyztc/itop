import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig
import configparser
import pdb

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransAppSolution():
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read("config.ini")        
        cmdb_db = self.cfg.get("cmdb","db")
        cmdb_str = self.cfg.get("cmdb","conn_str")
        self.client = MongoClient(cmdb_str)
        self.db = self.client[cmdb_db]
    
    def write_to_cmdb(self,coll_name,df):
        coll = self.db[coll_name]
        result =  coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name,str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" % (coll_name,str(len(result.inserted_ids))))

    def filter_project(self,item_name,keyword):
        if keyword in item_name:
            return item_name.split(keyword).pop()

    def get_hostname_list(self,host_list):
        hostname_list  = [ host.get("host") for host in host_list]
        return hostname_list


    def main(self):
        appsolution_coll = self.db['zabbix_project_servers']
        appsolution_df = pd.DataFrame(list(appsolution_coll.find()))
        merge_appsolution_df = pd.DataFrame()
        merge_appsolution_df['merge_name'] = appsolution_df['zabbix_name'].map(lambda x:self.filter_project(x,'Project: '))
        merge_appsolution_df['merge_environment'] = appsolution_df['zabbix_environment']
        merge_appsolution_df['merge_hosts'] = appsolution_df['zabbix_hosts'].map(lambda x:self.get_hostname_list(x))
        merge_appsolution_df = merge_appsolution_df[merge_appsolution_df['merge_name'].notnull()]

        appsolution_coll2 = self.db['merge_virtualmachine']
        appsolution_df2 = pd.DataFrame(list(appsolution_coll2.find()))
        temp = appsolution_df2.loc[appsolution_df2.merge_app != '',['merge_name','merge_app','merge_env']]
        temp['group_key'] = temp['merge_app'].values + '|'+ temp['merge_env'].values
        temp = temp.groupby('group_key').aggregate(lambda x: list(x))[['merge_name']].rename(columns={'merge_name':'merge_hosts'})
        temp=temp.reset_index()
        temp['merge_name'] = temp['group_key'].map(lambda x:x.split('|')[0].strip()) 
        temp['merge_environment'] = temp['group_key'].map(lambda x:x.split('|')[1].strip()) 
        merge_appsolution_df2=temp[['merge_name','merge_environment','merge_hosts']]
        # logger.info(merge_appsolution_df.head(20))
        merge_appsolution_df=merge_appsolution_df.append(merge_appsolution_df2)
        self.write_to_cmdb(coll_name='merge_appsolution',df=merge_appsolution_df)
        self.client.close()

if __name__ == '__main__':
    app = TransAppSolution()
    app.main()
