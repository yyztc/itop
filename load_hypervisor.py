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

class LoadHyper():

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

    def get_hyper_src_df(self,table_name):
        get_ps_sql = "select id as 'server_id',name,environment from %s" % (table_name)
        ps_df = pd.read_sql(get_ps_sql, con=self.engine).assign(join_name=lambda x:x['name'])
        vc_server_coll = self.db['vcenter_server']
        vc_server_df = pd.DataFrame(list(vc_server_coll.find())).assign(join_name=lambda x:x['vc_name'])
        vc_server_df['join_name'] = vc_server_df['join_name'].map(lambda x:str(x).lower().split('.cargosmart.com')[0])
        hyper_src_df = pd.merge(vc_server_df, ps_df, left_on='join_name', right_on='join_name', how='left').assign(primary_key=lambda x:x['name']).assign(org_id=lambda x:1)[['server_id','name','primary_key','org_id','environment']]
        # logger.info(hyper_src_df)
        return hyper_src_df


    def main(self):
        hyper_source_table = 'synchro_data_hypervisor_83'
        hyper_src_df = self.get_hyper_src_df(table_name='view_PhysicalServer')
        self.load_to_itopdb(df=hyper_src_df, source_table_name=hyper_source_table)
        self.apply_by_php(source_table_name=hyper_source_table)

if __name__ == '__main__':
    hyper = LoadHyper()
    hyper.main()

