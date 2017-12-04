from sqlalchemy import create_engine
import re
from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import subprocess as t
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class LoadDBServer():

    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']
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
        if table_name in ['view_PhysicalServer','view_VirtualMachine']:
            get_id_sql = "select id,name,environment from %s" % (table_name)
        else:
            get_id_sql = "select id,name from %s" % (table_name)
        id_df = pd.read_sql(get_id_sql, con=self.engine)
        id_df['id'] = id_df['id'].map(lambda x:str(int(x)))
        return id_df

    def get_dbserver_src_df(self):
        oracle_dbserver_coll = self.db['merge_oracle_db']
        mongo_dbserver_coll = self.db['merge_mongo_db']

        oracle_dbserver_df = pd.DataFrame(list(oracle_dbserver_coll.find())).assign(join_host_name=lambda x:x['merge_host_name'].str.upper())['merge_host_name']

        mongo_dbserver_df = pd.DataFrame(list(mongo_dbserver_coll.find())).rename(columns={'merge_hostname':'merge_host_name'}).assign(join_host_name=lambda x:x['merge_host_name'].str.upper())['merge_host_name']



        dbserver_df = pd.DataFrame(pd.concat([oracle_dbserver_df,mongo_dbserver_df]),columns=['merge_host_name']).assign(join_host_name=lambda x:x['merge_host_name'].str.upper())

        ps_id_df = self.get_id('view_PhysicalServer').assign(join_name=lambda x:x['name'].str.upper())
        vm_id_df = self.get_id('view_VirtualMachine').assign(join_name=lambda x:x['name'].str.upper())


        join1 = pd.merge(dbserver_df,ps_id_df,how='left',left_on='join_host_name',right_on='join_name')
        join2 = pd.merge(join1,vm_id_df,how='left',left_on='join_host_name',right_on='join_name')
        join3 = join2.assign(org_id=lambda x:1)

        join_prefix = 'join_'
        cols = [ col for col in join3.columns if join_prefix not in col]

        merge_prefix = 'merge_'
        col_map = [ col for col in cols if merge_prefix in col]

        col_dict = {}
        for col in col_map:
            col_dict[col]=col.split(merge_prefix)[1]

        dbserver_src_df = join3

        dbserver_src_df['merge_environment'] = dbserver_src_df.loc[dbserver_src_df['environment_x'].notnull(),'environment_x']

        dbserver_src_df.loc[dbserver_src_df['merge_environment'].isnull(),'merge_environment']= dbserver_src_df.loc[dbserver_src_df['environment_y'].notnull(),'environment_y']

        dbserver_src_df['system_id'] = dbserver_src_df.loc[dbserver_src_df['id_x'].notnull(),'id_x']

        dbserver_src_df.loc[dbserver_src_df['system_id'].isnull(),'system_id']= dbserver_src_df.loc[dbserver_src_df['id_y'].notnull(),'id_y']

        dbserver_src_df = dbserver_src_df[['merge_environment','merge_host_name','system_id','org_id']].rename(columns={'merge_environment':'environment','merge_host_name':'name'})

        return dbserver_src_df

    def main(self):
        dbserver_src_df = self.get_dbserver_src_df()
        self.load_to_itopdb(df=dbserver_src_df, source_table_name= 'synchro_data_dbserver_85')
        self.apply_by_php(source_table_name= 'synchro_data_dbserver_85')


if __name__ == '__main__':
    lb = LoadDBServer()
    lb.main()