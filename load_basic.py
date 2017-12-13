from sqlalchemy import create_engine
import re
from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import subprocess as t
import pdb
import logging
from logging.config import fileConfig
import configparser

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class LoadBasic():

    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read("config.ini")        
        cmdb_db = self.cfg.get("cmdb","db")
        cmdb_str = self.cfg.get("cmdb","conn_str")
        self.client = MongoClient(cmdb_str)
        self.db = self.client[cmdb_db]
        
        self.engine = create_engine(
            "mysql+pymysql://root:Password1@127.0.0.1:3306/itop?charset=utf8", encoding="utf-8", echo=False)

    def load_to_itopdb(self, df, source_table_name):
        self.engine.execute("delete from %s" % source_table_name)
        df.to_sql(source_table_name, con=self.engine,
                  if_exists='append', index=False)

    def apply_by_php(self, source_table_name):
        source_table_id = source_table_name.split('_').pop()
        php_cmd = "php -q /itop_data/http_dir/itop/synchro/synchro_exec.php --auth_user=%s --auth_pwd=%s --data_sources=%s" % (
            'admin', 'Password1', source_table_id)
        output = t.getoutput(php_cmd)
        logger.info(output + "\n")

    def load_location(self):
        location_source_table = 'synchro_data_location_77'
        location_coll = self.db['merge_location']
        location_df = pd.DataFrame(list(location_coll.find())).assign(org_id=lambda x: 1, name=lambda x: x[
            'merge_location'].str.upper()).replace('', 'OTHERS')[['org_id', 'name']]
        self.load_to_itopdb(
            df=location_df, source_table_name=location_source_table)
        self.apply_by_php(source_table_name=location_source_table)

    def load_brand(self):
        brand_source_table = 'synchro_data_brand_73'
        brand_coll = self.db['merge_brand']
        brand_df = pd.DataFrame(list(brand_coll.find())).assign(name=lambda x: x[
            'merge_brand'].str.upper()).assign(primary_key=lambda x: x['name']).replace('', 'OTHERS')[['name', 'primary_key']]
        self.load_to_itopdb(df=brand_df, source_table_name=brand_source_table)
        self.apply_by_php(source_table_name=brand_source_table)


    def load_model(self):
        get_brand_id_sql = "select id,name as brand_name from %s" % (
            'view_Brand')
        brand_id_df = pd.read_sql(get_brand_id_sql, con=self.engine).assign(
            brand_id=lambda x: x['id'].map(lambda y: str(int(y))))[['brand_id', 'brand_name']]

        model_source_table = 'synchro_data_model_74'
        model_coll = self.db['merge_model']
        model_df = pd.DataFrame(list(model_coll.find())).assign(name=lambda x: x['merge_model_name']).assign(
            primary_key=lambda x: x['name'].str.upper()).replace('', 'OTHERS')
        model_df = pd.merge(model_df, brand_id_df, how='left', left_on='merge_brand_name', right_on='brand_name').assign(type=lambda x:x['merge_model_type']).loc[model_df['name'] != 'OTHERS',['primary_key', 'brand_id', 'name','type']]
        
        self.load_to_itopdb(df=model_df, source_table_name=model_source_table)
        self.apply_by_php(source_table_name=model_source_table)

    def load_osfamily(self):
        osfamily_source_table = 'synchro_data_osfamily_75'
        osfamily_coll = self.db['merge_osfamily']
        osfamily_df = pd.DataFrame(list(osfamily_coll.find())).assign(name=lambda x: x[
            'merge_osfamily'].str.upper()).assign(primary_key=lambda x: x['name']).replace('', 'OTHERS')[['name', 'primary_key']]
        self.load_to_itopdb(df=osfamily_df, source_table_name=osfamily_source_table)
        self.apply_by_php(source_table_name=osfamily_source_table)

    def load_osversion(self):
        get_osfamily_id_sql = "select id,name as osfamily_name from %s" % (
            'view_OSFamily')
        osfamily_id_df = pd.read_sql(get_osfamily_id_sql, con=self.engine).assign(
            osfamily_id=lambda x: x['id'].map(lambda y: str(int(y))))[['osfamily_id', 'osfamily_name']]

        osversion_source_table = 'synchro_data_osversion_76'
        osversion_coll = self.db['merge_osversion']

        osversion_df = pd.DataFrame(list(osversion_coll.find())).assign(name=lambda x: x['merge_osversion']).assign(
            primary_key=lambda x: x['name'].str.upper()).replace('', 'OTHERS')

        osversion_df['merge_osfamily'] = osversion_df['merge_osfamily'].str.upper()

        osversion_df = pd.merge(osversion_df, osfamily_id_df, how='left', left_on='merge_osfamily', right_on='osfamily_name')
        # pdb.set_trace()

        osversion_df=osversion_df.loc[osversion_df['name'] != 'OTHERS',['primary_key', 'osfamily_id', 'name']]

        
        self.load_to_itopdb(df=osversion_df, source_table_name=osversion_source_table)
        self.apply_by_php(source_table_name=osversion_source_table)


    def load_network_type(self):
        network_type_source_table = 'synchro_data_networkdevicetype_81'
        network_type_coll = self.db['merge_network_type']
        network_type_df = pd.DataFrame(list(network_type_coll.find())).assign(org_id=lambda x: 1, name=lambda x: x[
            'merge_network_type'].str.upper()).replace('', 'OTHERS')[['name']]
        self.load_to_itopdb(
            df=network_type_df, source_table_name=network_type_source_table)
        self.apply_by_php(source_table_name=network_type_source_table)

    def main(self):
        self.load_location()
        self.load_brand()
        self.load_model()
        self.load_osfamily()
        self.load_osversion()
        self.load_network_type()

if __name__ == '__main__':
    lb = LoadBasic()
    lb.main()
