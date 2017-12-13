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

class LoadOC4J():

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

    def get_oc4j_src_df(self):
        oc4j_coll = self.db['merge_oc4j']
        oc4j_df = pd.DataFrame(list(oc4j_coll.find())).assign(join_server_name=lambda x:x['merge_server'].str.upper())

        middleware_id_df = self.get_id('view_Middleware').assign(join_name=lambda x:x['name'].str.upper())[['join_name','id']]

        oc4j_df = pd.merge(oc4j_df,middleware_id_df,how='left',left_on='join_server_name',right_on='join_name').rename(columns={'id':'middleware_id'})


        prefix = 'merge_'

        col_dict = {}
        for col in oc4j_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]

        # logger.info(oc4j_df)
        # logger.info(oc4j_df['merge_item'].unique())
        ['proc_num' 'oc4j_max_heap_size' 'oc4j_max_perm_size' 'oc4j_min_heap_size']

        oc4j_src_df = oc4j_df.rename(columns=col_dict).assign(org_id=lambda x:1).assign(primary_key=lambda x:x['name']).rename(columns={'item_value_m':'item_value'})

        oc4j_src_df =oc4j_src_df[oc4j_src_df['middleware_id'].notnull()]

        oc4j_max_heap_size_df = oc4j_src_df.loc[oc4j_src_df['item'] == 'oc4j_max_heap_size',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'max_heap_size'})[['join_name','max_heap_size']]
        oc4j_max_perm_size_df = oc4j_src_df.loc[oc4j_src_df['item'] == 'oc4j_max_perm_size',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'max_perm_size'})[['join_name','max_perm_size']]
        oc4j_oc4j_min_heap_size_df = oc4j_src_df.loc[oc4j_src_df['item'] == 'oc4j_min_heap_size',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'min_heap_size'})[['join_name','min_heap_size']]

        join1 = oc4j_src_df[oc4j_src_df['item'] == 'proc_num']
        join2 = pd.merge(join1,oc4j_max_heap_size_df,how='left',left_on='name',right_on='join_name')
        join3 = pd.merge(join2,oc4j_max_perm_size_df,how='left',left_on='name',right_on='join_name')
        join4 = pd.merge(join3,oc4j_oc4j_min_heap_size_df,how='left',left_on='name',right_on='join_name')
        
        oc4j_src_df = join4.rename(columns={'domain':'domain_name'})[['primary_key','name','org_id','middleware_id','environment','min_heap_size','max_heap_size','domain_name','max_perm_size']].drop_duplicates()

        # logger.info(oc4j_src_df.columns)

        return oc4j_src_df

    def main(self):
        oc4j_src_df = self.get_oc4j_src_df()
        self.load_to_itopdb(
            df=oc4j_src_df, source_table_name='synchro_data_oc4jinstance_95')
        self.apply_by_php(source_table_name='synchro_data_oc4jinstance_95')

# ['proc_num' 'oc4j_max_heap_size' 'oc4j_max_perm_size' 'oc4j_min_heap_size']

if __name__ == '__main__':
    oc4j = LoadOC4J()
    oc4j.main()
