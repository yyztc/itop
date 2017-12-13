import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransOHS():
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
        ohs_coll = self.db['zabbix_ohs']
        ohs_df = pd.DataFrame(list(ohs_coll.find())).fillna(value='')
        merge_ohs_df = pd.DataFrame()
        merge_ohs_df['merge_environment'] = ohs_df['zabbix_environment']
        merge_ohs_df['merge_server'] = ohs_df['zabbix_hosts'].map(lambda x:[it.get('host') for it in x][0])
        merge_ohs_df['merge_port'] = ohs_df['zabbix_key_'].map(lambda x:re.search(r'\D*(?P<port>\d+)\D*',x).groupdict().get('port'))
        merge_ohs_df['merge_name'] = ohs_df['zabbix_name'].map(lambda x:x.split()[-2])
        self.write_to_cmdb(coll_name='merge_ohs',df=merge_ohs_df)
        self.client.close()

if __name__ == '__main__':
    ohs = TransOHS()
    ohs.main()