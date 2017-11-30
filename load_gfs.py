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

class LoadGFS():

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

    def get_id(self, table_name):
        get_id_sql = "select id,name from %s" % (table_name)
        id_df = pd.read_sql(get_id_sql, con=self.engine)
        id_df['id'] = id_df['id'].map(lambda x: str(int(x)))
        return id_df

    def get_gfs_src_df(self):
        gfs_coll = self.db['merge_gfs']
        gfs_df = pd.DataFrame(list(gfs_coll.find())).assign(join_server_name=lambda x:x['merge_server'].str.upper())
        middleware_id_df = self.get_id('view_Middleware').assign(join_name=lambda x:x['name'].str.upper())[['join_name','id']]
        gfs_df = pd.merge(gfs_df,middleware_id_df,how='left',left_on='join_server_name',right_on='join_name').rename(columns={'id':'middleware_id'})

        prefix = 'merge_'
        col_dict = {}
        for col in gfs_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]

        gfs_src_df = gfs_df.rename(columns=col_dict).assign(name=lambda x:x['server']).assign(primary_key=lambda x:x['name']).assign(org_id=lambda x:1)[['environment','name','middleware_id','primary_key','org_id']].drop_duplicates()

        gfs_src_df=gfs_src_df[gfs_src_df['middleware_id'].notnull()]

        # logger.info(gfs_src_df.head(10))

        return gfs_src_df

    def main(self):
        gfs_src_df = self.get_gfs_src_df()
        self.load_to_itopdb(
            df=gfs_src_df, source_table_name='synchro_data_gfsinstance_99')
        self.apply_by_php(source_table_name='synchro_data_gfsinstance_99')


if __name__ == '__main__':
    sf = LoadGFS()
    sf.main()
