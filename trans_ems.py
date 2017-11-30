import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransEMS():
    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']

    def write_to_cmdb(self,coll_name,df):
        coll = self.db[coll_name]
        result =  coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name,str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" % (coll_name,str(len(result.inserted_ids))))

    def main(self):
        ems_coll = self.db['zabbix_ems']
        ems_df = pd.DataFrame(list(ems_coll.find())).fillna(value='')
        merge_ems_df = pd.DataFrame()
        merge_ems_df['merge_environment'] = ems_df['zabbix_environment']
        merge_ems_df['merge_server'] = ems_df['zabbix_hosts'].map(lambda x:[it.get('host') for it in x][0])
        merge_ems_df['merge_name'] = ems_df['zabbix_name'].map(lambda x:x.split()[-2])
        merge_ems_df['merge_port'] = ems_df['zabbix_key_'].map(lambda x:re.search(r'\D*(?P<port>\d+)\D*',x).groupdict().get('port'))
        self.write_to_cmdb(coll_name='merge_ems',df=merge_ems_df)
        self.client.close()

if __name__ == '__main__':
    ems = TransEMS()
    ems.main()
