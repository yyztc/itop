from sqlalchemy import create_engine
import re
from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import subprocess as t
import logging
from logging.config import fileConfig
import configparser

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class LoadAppsolution():

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

    def get_id(self, table_name):
        get_id_sql = "select id,name from %s" % (table_name)
        id_df = pd.read_sql(get_id_sql, con=self.engine)
        id_df['id'] = id_df['id'].map(lambda x: str(int(x)))
        return id_df

    def get_appsolution_src_df(self):
        appsolution_coll = self.db['merge_appsolution']
        appsolution_df = pd.DataFrame(list(appsolution_coll.find()))
        prefix = 'merge_'
        col_dict = {}
        for col in appsolution_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]
        appsolution_src_df = appsolution_df.rename(columns=col_dict)[['environment','name']].assign(org_id=lambda x:1).assign(primary_key=lambda x:x['name'])
        # logger.info(appsolution_src_df.head(20))
        return appsolution_src_df

    def get_appsolution_src_df2(self):
        vm_coll = self.db['merge_virtualmachine']
        vm_df = pd.DataFrame(list(vm_coll.find()))
        appsolution_src_df = vm_df[['merge_env','merge_app']]
        appsolution_src_df=appsolution_src_df.rename(columns={"merge_env":"environment","merge_app":"name"}).assign(org_id=lambda x:1).assign(primary_key=lambda x:x['name'])
        appsolution_src_df2=appsolution_src_df[appsolution_src_df.name!='']
        return appsolution_src_df2


    def main(self):
        appsolution_src_df = self.get_appsolution_src_df()
        self.load_to_itopdb(
            df=appsolution_src_df, source_table_name='synchro_data_applicationsolution_101')
        self.apply_by_php(source_table_name='synchro_data_applicationsolution_101')

        appsolution_src_df2 = self.get_appsolution_src_df2()
        self.load_to_itopdb(
            df=appsolution_src_df2, source_table_name='synchro_data_applicationsolution_101')
        self.apply_by_php(source_table_name='synchro_data_applicationsolution_101')

if __name__ == '__main__':
    appsolution = LoadAppsolution()
    appsolution.main()
