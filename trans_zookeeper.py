import re
from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransZK():

    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']

    def write_to_cmdb(self, coll_name, df):
        coll = self.db[coll_name]
        result = coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" %
              (coll_name, str(len(result.inserted_ids))))

    def main(self):
        zookeeper_coll = self.db['zabbix_zookeeper']
        zookeeper_df = pd.DataFrame(
            list(zookeeper_coll.find())).fillna(value='')
        merge_zookeeper_df = pd.DataFrame()
        merge_zookeeper_df['merge_environment'] = zookeeper_df[
            'zabbix_environment']
        merge_zookeeper_df['merge_server'] = zookeeper_df[
            'zabbix_hosts'].map(lambda x: [it.get('host') for it in x][0])
        merge_zookeeper_df['merge_port'] = zookeeper_df['zabbix_key_'].map(
            lambda x: re.search(r'\D*(?P<port>\d+)\D*', x).        groupdict().get('port'))
        merge_zookeeper_df['merge_name'] = merge_zookeeper_df[
            'merge_server'].values + ':' + merge_zookeeper_df['merge_port'].      values
        self.write_to_cmdb(coll_name='merge_zookeeper', df=merge_zookeeper_df)
        self.client.close()

if __name__ == '__main__':
    zk = TransZK()
    zk.main()
