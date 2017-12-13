import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransGFS():
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
        gfs_coll = self.db['zabbix_gfs']
        gfs_df = pd.DataFrame(list(gfs_coll.find())).fillna(value='')
        merge_gfs_df = pd.DataFrame()
        merge_gfs_df['merge_environment'] = gfs_df['zabbix_environment']
        merge_gfs_df['merge_server'] = gfs_df['zabbix_hosts'].map(lambda x:[it.get('host') for it in x][0])
        self.write_to_cmdb(coll_name='merge_gfs',df=merge_gfs_df)
        self.client.close()

if __name__ == '__main__':
    gfs = TransGFS()
    gfs.main()