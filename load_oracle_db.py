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

class LoadOracleDB():

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

    def get_oracle_db_src_df(self):
        oracle_db_coll = self.db['merge_oracle_db']
        oracle_db_df = pd.DataFrame(list(oracle_db_coll.find())).assign(
            join_dbserver_name=lambda x: x['merge_host_name'].str.upper())
        dbserver_id_df = self.get_id(table_name='view_DBServer').assign(
            join_dbserver_name=lambda x: x['name'].str.upper())

        oracle_db_src_df = pd.merge(oracle_db_df, dbserver_id_df, how='left', left_on='join_dbserver_name', right_on='join_dbserver_name')[['merge_environment', 'merge_home_location', 'merge_home_name', 'merge_name', 'merge_version', 'id']].rename(
            columns={'id': 'dbserver_id', 'merge_environment': 'environment', 'merge_home_location': 'home_path', 'merge_home_name': 'home_name', 'merge_name': 'name', 'merge_version': 'version'}).assign(primary_key=lambda x: x['name']).assign(org_id=lambda x:1)

        # logger.info(oracle_db_src_df.head(20))

        return oracle_db_src_df

    def main(self):
        oracle_db_src_df = self.get_oracle_db_src_df()
        self.load_to_itopdb(
            df=oracle_db_src_df, source_table_name='synchro_data_oracledbinstance_86')
        self.apply_by_php(source_table_name='synchro_data_oracledbinstance_86')


if __name__ == '__main__':
    db = LoadOracleDB()
    db.main()
