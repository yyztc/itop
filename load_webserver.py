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

class LoadWebServer():

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

    def get_id(self,table_name):
        get_id_sql = "select id,name from %s" % (table_name)
        id_df = pd.read_sql(get_id_sql, con=self.engine)
        id_df['id'] = id_df['id'].map(lambda x:str(int(x)))
        return id_df

    def get_webserver_src_df(self):
        spotfireweb_coll = self.db['merge_spotfireweb']
        nginx_coll = self.db['merge_nginx']
        ohs_coll = self.db['merge_ohs']

        spotfireweb_df = pd.DataFrame(list(spotfireweb_coll.find()))
        nginx_df = pd.DataFrame(list(nginx_coll.find()))
        ohs_df = pd.DataFrame(list(ohs_coll.find()))

        ps_id_df = self.get_id('view_PhysicalServer').assign(join_name=lambda x:x['name'].str.upper())
        vm_id_df = self.get_id('view_VirtualMachine').assign(join_name=lambda x:x['name'].str.upper())
        server_id_df = pd.concat([ps_id_df,vm_id_df])[['id','join_name']]

        webserver_df = pd.DataFrame(pd.concat([pd.concat([spotfireweb_df,nginx_df]),ohs_df])).assign(join_server_name=lambda x:x['merge_server'].str.upper())

        webserver_df = pd.merge(webserver_df,server_id_df,how='left',left_on='join_server_name',right_on='join_name').rename(columns={'id':'system_id'})

        prefix = 'merge_'

        col_dict = {}
        for col in webserver_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]

        webserver_df = webserver_df.rename(columns=col_dict)[['environment','server','system_id']].rename(columns={'server':'name'}).assign(org_id=lambda x:1).assign(primary_key=lambda x:x['name'])
        webserver_src_df = webserver_df[webserver_df['system_id'].notnull()].drop_duplicates()

        return webserver_src_df

    def main(self):
        webserver_src_df = self.get_webserver_src_df()
        self.load_to_itopdb(df=webserver_src_df, source_table_name= 'synchro_data_webserver_88')
        self.apply_by_php(source_table_name= 'synchro_data_webserver_88')


if __name__ == '__main__':
    ws = LoadWebServer()
    ws.main()