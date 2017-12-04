
import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransStorage():
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
        excel_st_coll = self.db['excel_storage']
        excel_st_df = pd.DataFrame(list(excel_st_coll.find())).fillna(value='')
        merge_st_df = pd.DataFrame()
        merge_st_df['merge_function'] = excel_st_df['excel_function'].str.replace('UNKOWN','')
        merge_st_df['merge_check_by'] = excel_st_df['excel_check_by']
        merge_st_df['merge_check_date'] = excel_st_df['excel_check_date']
        merge_st_df['merge_location'] = excel_st_df['excel_location']
        merge_st_df['merge_maint_period'] = excel_st_df['excel_maint_period']
        merge_st_df['merge_maint_vendor'] = excel_st_df['excel_maint_vendor']
        merge_st_df['merge_model_name'] = excel_st_df['excel_model_name']
        merge_st_df['merge_power_status'] = excel_st_df['excel_power_status']
        merge_st_df['merge_rack_location'] = excel_st_df['excel_rack_location']
        merge_st_df['merge_remarks'] = excel_st_df['excel_remarks']
        merge_st_df['merge_serial_num'] = excel_st_df['excel_serial_num']
        merge_st_df['merge_type'] = excel_st_df['excel_type']
        merge_st_df['merge_vendor'] = excel_st_df['excel_vendor']
        self.write_to_cmdb(coll_name='merge_storage',df=merge_st_df)
        self.client.close()

if __name__ == '__main__':
    s = TransStorage()
    s.main()