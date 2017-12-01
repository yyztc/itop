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

class LoadLinkAppsolution():

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

    def get_link_appsolution_src_df(self):
        appsolution_coll = self.db['merge_appsolution']
        link_appsolution_df = pd.DataFrame(list(appsolution_coll.find()))
        prefix = 'merge_'
        col_dict = {}
        for col in link_appsolution_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]                
        link_appsolution_df = link_appsolution_df.rename(columns=col_dict)[['environment','name','hosts']].assign(join_appsolution_name=lambda x:x['name'].str.upper())

        appsolution_id_df = self.get_id(table_name='view_ApplicationSolution').assign(join_appsolution_name=lambda x:x['name'].str.upper())[['id','join_appsolution_name']]
        vm_id_df = self.get_id(table_name='view_VirtualMachine').assign(join_vm_name=lambda x:x['name'].str.upper()).rename(columns={'id':'vm_id'})[['vm_id','join_vm_name']]
        ps_id_df = self.get_id(table_name='view_PhysicalServer').assign(join_ps_name=lambda x:x['name'].str.upper()).rename(columns={'id':'ps_id'})[['ps_id','join_ps_name']]

        join1 = pd.merge(link_appsolution_df,appsolution_id_df,how='left',left_on='join_appsolution_name',right_on='join_appsolution_name').rename(columns={'id':'applicationsolution_id'})
        split_join1 = join1['hosts'].map(lambda x:','.join(x)).str.split(',', expand=True).stack().reset_index(level=0).set_index('level_0').rename(columns={0:'host'}).join(join1.drop('hosts', axis=1))[['environment','name','host','applicationsolution_id']].assign(join_host_name=lambda x:x['host'].str.upper())
        join2 = pd.merge(split_join1,vm_id_df,how='left',left_on='join_host_name',right_on='join_vm_name')
        join3 = pd.merge(join2,ps_id_df,how='left',left_on='join_host_name',right_on='join_ps_name')

        link_appsolution_src_df = join3.assign(primary_key=lambda x:x['name'])
        link_appsolution_src_df['functionalci_id'] = link_appsolution_src_df['vm_id']
        link_appsolution_src_df.loc[link_appsolution_src_df['functionalci_id'].isnull(),'functionalci_id'] = link_appsolution_src_df.loc[link_appsolution_src_df['ps_id'].notnull(),'ps_id']
        link_appsolution_src_df = link_appsolution_src_df[['primary_key','applicationsolution_id','functionalci_id']].drop_duplicates()
        return link_appsolution_src_df


    def main(self):
        link_appsolution_src_df = self.get_link_appsolution_src_df()
        self.load_to_itopdb(
            df=link_appsolution_src_df, source_table_name='synchro_data_lnkapplicationsolutiontofunctionalci_102')
        self.apply_by_php(source_table_name='synchro_data_lnkapplicationsolutiontofunctionalci_102')

if __name__ == '__main__':
    link = LoadLinkAppsolution()
    link.main()
