import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransWLS():
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
        weblogic_coll = self.db['zabbix_weblogic']
        weblogic_df = pd.DataFrame(list(weblogic_coll.find())).fillna(value='')
        merge_weblogic_df = pd.DataFrame()
        merge_weblogic_df['merge_environment'] = weblogic_df['zabbix_environment']
        merge_weblogic_df['merge_server'] = weblogic_df['zabbix_hosts'].map(lambda x:[it.get('host') for it in x][0])
        merge_weblogic_df['merge_name'] = weblogic_df['zabbix_name'].map(lambda x:x.split()[-1])
        merge_weblogic_df['merge_domain'] = weblogic_df['zabbix_name'].map(lambda x:x.split()[-2])
        merge_weblogic_df['merge_item'] = weblogic_df['zabbix_key_'].map(lambda x:re.search(r'(?P<item>.*)\[.*',x).        groupdict().get('item').replace('.','_'))
        merge_weblogic_df['merge_item_value'] = weblogic_df['zabbix_prevvalue'].map(lambda x:re.search(r'(?P<item_value>\d*)\w*'        ,x).groupdict().get('item_value'))
        self.write_to_cmdb(coll_name='merge_weblogic',df=merge_weblogic_df)
        self.client.close()


if __name__ == '__main__':
    wls = TransWLS()
    wls.main()