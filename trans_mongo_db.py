import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransMongo():
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
        mongo_db_coll = self.db['ops_database']
        mongo_db_df = pd.DataFrame(list(mongo_db_coll.find())).fillna(value='')
        merge_db_df = pd.DataFrame()
        merge_db_df['merge_authMechanismName'] = mongo_db_df['ops_authMechanismName'].str.replace('NONE','')
        merge_db_df['merge_created'] = mongo_db_df['ops_created']
        merge_db_df['merge_hidden'] = mongo_db_df['ops_hidden']
        merge_db_df['merge_hostname'] = mongo_db_df['ops_hostname'].map(lambda x:x.lower().split('.cargosmart.com')[0])
        merge_db_df['merge_journalingEnabled'] = mongo_db_df['ops_journalingEnabled']
        merge_db_df['merge_logsEnabled'] = mongo_db_df['ops_logsEnabled']
        merge_db_df['merge_name'] = mongo_db_df['ops_name'].str.replace('.cargosmart.com','')
        merge_db_df['merge_port'] = mongo_db_df['ops_port']
        merge_db_df['merge_profilerEnabled'] = mongo_db_df['ops_profilerEnabled']
        merge_db_df['merge_replicaSetName'] = mongo_db_df['ops_replicaSetName']
        merge_db_df['merge_replicaStateName'] = mongo_db_df['ops_replicaStateName']
        merge_db_df['merge_sslEnabled'] = mongo_db_df['ops_sslEnabled']
        merge_db_df['merge_typeName'] = mongo_db_df['ops_typeName']
        merge_db_df['merge_version'] = mongo_db_df['ops_version']
        self.write_to_cmdb(coll_name='merge_mongo_db',df=merge_db_df)
        self.client.close()

if __name__ == '__main__':
    mongo = TransMongo()
    mongo.main()

