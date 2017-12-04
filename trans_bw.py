import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransBW():
    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']
        bw_coll = self.db['zabbix_bw']
        self.bw_df = pd.DataFrame(list(bw_coll.find())).fillna(value='')

    def get_app_inst_name(self,name):
        sep_list = [s for s in name if s=='-']
        replace_num = int(len(sep_list)/2)
        temp_list = name.replace('-','=',replace_num).split('-',1)
        app_name = temp_list[0].replace('=','-')
        inst_name = temp_list[1]
        return (app_name,inst_name)
    
    def write_to_cmdb(self,coll_name,df):
        coll = self.db[coll_name]
        result =  coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name,str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" % (coll_name,str(len(result.inserted_ids))))
    
    def main(self):
        merge_bw_df = pd.DataFrame()
        merge_bw_df['merge_environment'] = self.bw_df['zabbix_environment']
        merge_bw_df['merge_server'] = self.bw_df['zabbix_hosts'].map(lambda x:[it.get('host') for it in x][0])
        merge_bw_df['merge_app'] = self.bw_df['zabbix_name'].map(lambda x:self.get_app_inst_name(x.split()[-1])[0])
        merge_bw_df['merge_inst'] = self.bw_df['zabbix_name'].map(lambda x:self.get_app_inst_name(x.split()[-1])[1])
        self.write_to_cmdb(coll_name='merge_bw',df=merge_bw_df)
        self.client.close()

if __name__ == '__main__':
    bw = TransBW()
    bw.main()
