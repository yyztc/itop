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

class LoadMiddleware():

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

    def get_middleware_src_df(self):
        bw_coll = self.db['merge_bw']
        ems_coll = self.db['merge_ems']
        gfs_coll = self.db['merge_gfs']
        oc4j_coll = self.db['merge_oc4j']
        solr_coll = self.db['merge_solr']
        spotfire_coll = self.db['merge_spotfire']
        weblogic_coll = self.db['merge_weblogic']
        zookeeper_coll = self.db['merge_zookeeper']

        bw_df = pd.DataFrame(list(bw_coll.find()))[['merge_server','merge_environment']]
        ems_df = pd.DataFrame(list(ems_coll.find()))[['merge_server','merge_environment']]
        gfs_df = pd.DataFrame(list(gfs_coll.find()))[['merge_server','merge_environment']]
        oc4j_df = pd.DataFrame(list(oc4j_coll.find()))[['merge_server','merge_environment']]
        solr_df = pd.DataFrame(list(solr_coll.find()))[['merge_server','merge_environment']]
        spotfire_df = pd.DataFrame(list(spotfire_coll.find()))[['merge_server','merge_environment']]
        weblogic_df = pd.DataFrame(list(weblogic_coll.find()))[['merge_server','merge_environment']]
        zookeeper_df = pd.DataFrame(list(zookeeper_coll.find()))[['merge_server','merge_environment']]

        ps_id_df = self.get_id('view_PhysicalServer').assign(join_name=lambda x:x['name'].str.upper())
        vm_id_df = self.get_id('view_VirtualMachine').assign(join_name=lambda x:x['name'].str.upper())
        server_id_df = pd.concat([ps_id_df,vm_id_df])[['id','join_name']]

        middleware_df = pd.DataFrame(pd.concat([bw_df,ems_df,gfs_df,oc4j_df,solr_df,spotfire_df,weblogic_df,zookeeper_df])).assign(join_server_name=lambda x:x['merge_server'].str.upper())

        middleware_df = pd.merge(middleware_df,server_id_df,how='left',left_on='join_server_name',right_on='join_name').rename(columns={'id':'system_id'})

        # logger.info(middleware_df.head(20))

        prefix = 'merge_'

        col_dict = {}
        for col in middleware_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]

        middleware_df = middleware_df.rename(columns=col_dict)[['environment','server','system_id']].rename(columns={'server':'name'}).assign(org_id=lambda x:1).assign(primary_key=lambda x:x['name'])
        middleware_src_df = middleware_df[middleware_df['system_id'].notnull()].drop_duplicates()

        return middleware_src_df

    def main(self):
        middleware_src_df = self.get_middleware_src_df()
        self.load_to_itopdb(df=middleware_src_df, source_table_name= 'synchro_data_middleware_92')
        self.apply_by_php(source_table_name= 'synchro_data_middleware_92')


if __name__ == '__main__':
    ws = LoadMiddleware()
    ws.main()