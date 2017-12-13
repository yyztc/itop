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

class LoadWLS():

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

    def get_weblogic_src_df(self):
        weblogic_coll = self.db['merge_weblogic']
        weblogic_df = pd.DataFrame(list(weblogic_coll.find())).assign(join_server_name=lambda x:x['merge_server'].str.upper())
        middleware_id_df = self.get_id('view_Middleware').assign(join_name=lambda x:x['name'].str.upper())[['join_name','id']]
        weblogic_df = pd.merge(weblogic_df,middleware_id_df,how='left',left_on='join_server_name',right_on='join_name').rename(columns={'id':'middleware_id'})

        prefix = 'merge_'
        col_dict = {}
        for col in weblogic_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]

        weblogic_df = weblogic_df.rename(columns=col_dict)[['domain','environment','item','item_value','name','middleware_id']].assign(join_name=lambda x:x['name'].str.upper())
        
        wls_max_heap_size_df = weblogic_df.loc[weblogic_df['item']=='wls_max_heap_size',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'max_heap_size'})[['join_name','max_heap_size']]
        wls_max_perm_size_df = weblogic_df.loc[weblogic_df['item']=='wls_max_perm_size',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'max_perm_size'})[['join_name','max_perm_size']]
        wls_min_heap_size_df = weblogic_df.loc[weblogic_df['item']=='wls_min_heap_size',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'min_heap_size'})[['join_name','min_heap_size']]
        wls_listen_port_df = weblogic_df.loc[weblogic_df['item']=='wls_listen_port',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'listen_port'})[['join_name','listen_port']]
        wls_version_df = weblogic_df.loc[weblogic_df['item']=='wls_version',['name','item_value']].assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'item_value':'version'})[['join_name','version']]

        join1 = weblogic_df[weblogic_df['item'] == 'proc_num']
        join2 = pd.merge(join1,wls_max_heap_size_df,how='left',left_on='join_name',right_on='join_name')
        join3 = pd.merge(join2,wls_max_perm_size_df,how='left',left_on='join_name',right_on='join_name')
        join4 = pd.merge(join3,wls_min_heap_size_df,how='left',left_on='join_name',right_on='join_name')
        join5 = pd.merge(join4,wls_listen_port_df,how='left',left_on='join_name',right_on='join_name')
        join6 = pd.merge(join5,wls_version_df,how='left',left_on='join_name',right_on='join_name')
        weblogic_src_df = join6.assign(primary_key=lambda x:x['name']).assign(org_id=lambda x:1).rename(columns={'domain':'domain_name'})[['primary_key','name','org_id','middleware_id','environment','min_heap_size','max_heap_size','domain_name','max_perm_size','listen_port']].drop_duplicates()

        # logger.info(weblogic_src_df.head(10))

        return weblogic_src_df

    def main(self):
        weblogic_src_df = self.get_weblogic_src_df()
        self.load_to_itopdb(
            df=weblogic_src_df, source_table_name='synchro_data_weblogicinstance_97')
        self.apply_by_php(source_table_name='synchro_data_weblogicinstance_97')


if __name__ == '__main__':
    wls = LoadWLS()
    wls.main()
