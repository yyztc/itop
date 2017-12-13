from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig
import configparser

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransBasic():

    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read("config.ini")        
        cmdb_db = self.cfg.get("cmdb","db")
        cmdb_str = self.cfg.get("cmdb","conn_str")
        self.client = MongoClient(cmdb_str)
        self.db = self.client[cmdb_db]

        ps_coll = self.db['merge_phisical_server']
        nw_coll = self.db['merge_network']
        st_coll = self.db['merge_storage']
        vm_coll = self.db['merge_virtualmachine']
        self.ps_df = pd.DataFrame(list(ps_coll.find())).fillna(value='OTHERS').assign(merge_model_type=lambda x:'Server')
        self.nw_df = pd.DataFrame(list(nw_coll.find())).fillna(value='OTHERS').assign(merge_model_type=lambda x:'NetworkDevice')
        self.st_df = pd.DataFrame(list(st_coll.find())).fillna(value='OTHERS').assign(merge_model_type=lambda x:'StorageSystem')
        self.vm_df = pd.DataFrame(list(vm_coll.find())).fillna(value='OTHERS')

    def get_basic_df(self):
        self.merge_location_df = pd.DataFrame(concat([concat([self.ps_df['merge_location'], self.nw_df['merge_location']]), self.st_df['merge_location']]).unique(),columns=['merge_location'])
        self.merge_brand_df = pd.DataFrame(concat([concat([self.ps_df['merge_brand_name'], self.nw_df['merge_brand_name']]), self.st_df['merge_vendor']]).unique(),columns = ['merge_brand'])
        self.merge_network_type_df = pd.DataFrame(self.nw_df['merge_brand_name'].rename(columns={'merge_brand_name':'merge_network_type'}).unique(),columns=['merge_network_type'])
        self.merge_model_df = pd.DataFrame(concat([concat([self.ps_df[['merge_model_name','merge_brand_name','merge_model_type']], self.nw_df[['merge_model_name','merge_brand_name','merge_model_type']]]), self.st_df.rename(columns={'merge_vendor':'merge_brand_name'})[['merge_model_name','merge_brand_name','merge_model_type']]])).drop_duplicates()
        self.merge_osfamily_df = pd.DataFrame(concat([self.ps_df['merge_osvendor'], self.vm_df['merge_osfamily']]).unique(),columns=['merge_osfamily'])
        self.merge_osversion_df = pd.DataFrame(concat([self.ps_df.rename(columns={'merge_osvendor':'merge_osfamily','merge_osversion_name':'merge_osversion'})[['merge_osversion','merge_osfamily']], self.vm_df[['merge_osversion','merge_osfamily']]])).drop_duplicates()

    def write_to_cmdb(self,coll_name, df):
        coll = self.db[coll_name]
        result = coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" %
                (coll_name, str(len(result.inserted_ids))))

    def main(self):
        self.get_basic_df()
        self.write_to_cmdb('merge_location',self.merge_location_df)
        self.write_to_cmdb('merge_brand',self.merge_brand_df)
        self.write_to_cmdb('merge_model',self.merge_model_df)
        self.write_to_cmdb('merge_osfamily',self.merge_osfamily_df)
        self.write_to_cmdb('merge_osversion',self.merge_osversion_df)
        self.write_to_cmdb('merge_network_type',self.merge_network_type_df)

if __name__ == '__main__':
    tb = TransBasic()
    tb.main()