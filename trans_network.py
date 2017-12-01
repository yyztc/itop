
import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransNetwork():
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
        excel_network_coll = self.db['excel_network']
        excel_network_df = pd.DataFrame(list(excel_network_coll.find())).fillna(value='')
        merge_network_df = pd.DataFrame()
        merge_network_df['merge_name'] = excel_network_df['excel_hostname'].map(lambda x:x.lower())
        merge_network_df['merge_ip'] = excel_network_df['excel_ip'].str.replace('-','')
        merge_network_df['merge_model_name'] = excel_network_df['excel_model_name'].map(lambda x:x.upper().strip())
        merge_network_df['merge_brand_name'] = excel_network_df['excel_brand_name'].map(lambda x:x.upper())
        merge_network_df['merge_location'] = excel_network_df['excel_location']
        merge_network_df['merge_rack_location'] = excel_network_df['excel_rack_location']
        merge_network_df['merge_serial_num'] = excel_network_df['excel_serial_num']
        merge_network_df['merge_maint_required'] = excel_network_df['excel_maint_required']
        merge_network_df['merge_eol_date'] = excel_network_df['excel_eol_date']
        merge_network_df['merge_use_status'] = excel_network_df['excel_use_status']
        merge_network_df['merge_remarks'] = excel_network_df['excel_remarks']
        merge_network_df['merge_check_date'] = excel_network_df['excel_check_date']
        merge_network_df['merge_check_by'] = excel_network_df['excel_check_by']
        self.write_to_cmdb(coll_name='merge_network',df=merge_network_df)
        self.client.close()

if __name__ == '__main__':
    nw = TransNetwork()
    nw.main()

