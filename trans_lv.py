import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransLV():
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
        lv_coll = self.db['vcenter_logicalvolume']
        lv_df = pd.DataFrame(list(lv_coll.find())).fillna(value='')
        
        merge_lv_df = pd.DataFrame()
        merge_lv_df['merge_capacity_m'] = lv_df['vc_capacity'].map(lambda x:str(int(x/1024/1024)))
        merge_lv_df['merge_freespace_m'] = lv_df['vc_freespace'].map(lambda x:str(int(x/1024/1024)))
        merge_lv_df['merge_hosts'] = lv_df['vc_hosts'].map(lambda x:[host.lower().split('.cargosmart.com')[0] for host in x]    )
        merge_lv_df['merge_name'] = lv_df['vc_name'].map(lambda x:x.lower())
        merge_lv_df['merge_type'] = lv_df['vc_type']
        merge_lv_df['merge_vmfs_version'] = lv_df['vc_vmfs_version']
        merge_lv_df['merge_vms'] = lv_df['vc_vms']
        self.write_to_cmdb(coll_name='merge_logicalvolume',df=merge_lv_df)
        self.client.close()

if __name__ == '__main__':
    lv = TransLV()
    lv.main()
