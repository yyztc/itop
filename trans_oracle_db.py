import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig
import configparser

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransOracle():
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

    def main(self):
        oem_db_coll = self.db['oem_database']
        oem_db_df = pd.DataFrame(list(oem_db_coll.find())).fillna(value='')
        merge_db_df = pd.DataFrame()
        merge_db_df['merge_environment'] = oem_db_df['oem_environment']
        merge_db_df['merge_home_location'] = oem_db_df['oem_home_location']
        merge_db_df['merge_home_name'] = oem_db_df['oem_home_name']
        merge_db_df['merge_name'] = oem_db_df['oem_name']
        merge_db_df['merge_software'] = oem_db_df['oem_software']
        merge_db_df['merge_version'] = oem_db_df['oem_version']
        merge_db_df['merge_host_name'] = oem_db_df['oem_host_name'].map(lambda x:x.lower().split('.cargosmart.com')[0])
        self.write_to_cmdb(coll_name='merge_oracle_db',df=merge_db_df)
        self.client.close()

if __name__ == '__main__':
    oracle = TransOracle()
    oracle.main()
