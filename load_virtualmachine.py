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

class LoadVM():

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
        if table_name != 'view_Hypervisor':
            get_id_sql = "select id,name from %s" % (table_name)
        else:
            get_id_sql = "select id,name,environment from %s" % (table_name)
        id_df = pd.read_sql(get_id_sql, con=self.engine)
        id_df['id'] = id_df['id'].map(lambda x:str(int(x)))
        return id_df

    def get_vm_src_df(self):
        osfamily_id_df=self.get_id('view_OSFamily').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'osfamily_id'})[['join_name','osfamily_id']]
        osversion_id_df=self.get_id('view_OSVersion').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'osversion_id'})[['join_name','osversion_id']]
        hypervisor_id_df=self.get_id('view_Hypervisor').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'virtualhost_id'})[['join_name','virtualhost_id','environment']]

        # logger.info(osfamily_id_df.head(5))
        # logger.info(osversion_id_df.head(5))
        # logger.info(hypervisor_id_df.head(5))
        
        vm_coll = self.db['merge_virtualmachine']
        vm_df = pd.DataFrame(list(vm_coll.find())).assign(join_osfamily_name=lambda x:x['merge_osfamily'].str.upper()).assign(join_osversion_name=lambda x:x['merge_osversion'].str.upper()).assign(join_physicalserver_name=lambda x:x['merge_physical_server'].str.upper())

        join1 = pd.merge(vm_df,osfamily_id_df,how='left',left_on='join_osfamily_name',right_on='join_name')
        join2 = pd.merge(join1,osversion_id_df,how='left',left_on='join_osversion_name',right_on='join_name')
        join3 = pd.merge(join2,hypervisor_id_df,how='left',left_on='join_physicalserver_name',right_on='join_name')
        join4 = join3.assign(org_id=lambda x:1)

        join_prefix = 'join_'
        cols = [ col for col in join4.columns if join_prefix not in col]
        cols.remove('_id')

        merge_prefix = 'merge_'
        col_map = [ col for col in cols if merge_prefix in col]

        col_dict = {}
        for col in col_map:
            col_dict[col]=col.split(merge_prefix)[1]

        vm_src_df = join4[cols].rename(columns=col_dict).rename(columns={'cpu_num': 'cpu','mem_size': 'ram','power_status': 'powerState'}).assign(primary_key=lambda x:x['name'])[['org_id','virtualhost_id','osfamily_id','osversion_id','primary_key','name','cpu','ram','environment','powerState','ip']]

        vm_src_df = vm_src_df[vm_src_df['powerState'] == 'POWEREDON']

        # logger.info(vm_src_df.head(5))
        return vm_src_df

    def main(self):
        vm_source_table = 'synchro_data_virtualmachine_84'
        vm_src_df = self.get_vm_src_df()
        self.load_to_itopdb(df=vm_src_df, source_table_name=vm_source_table)
        self.apply_by_php(source_table_name=vm_source_table)

if __name__ == '__main__':
    vm = LoadVM()
    vm.main()

