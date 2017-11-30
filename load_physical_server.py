from sqlalchemy import create_engine
import re
from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import subprocess as t
import pdb
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class LoadPS():

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
        get_id_sql = "select id,name from %s" % (table_name)
        id_df = pd.read_sql(get_id_sql, con=self.engine)
        id_df['id'] = id_df['id'].map(lambda x:str(int(x)))
        return id_df

    def get_ps_src_df(self):
        brand_id_df=self.get_id('view_Brand').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'brand_id'})[['join_name','brand_id']]
        model_id_df=self.get_id('view_Model').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'model_id'})[['join_name','model_id']]
        osfamily_id_df=self.get_id('view_OSFamily').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'osfamily_id'})[['join_name','osfamily_id']]
        osversion_id_df=self.get_id('view_OSVersion').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'osversion_id'})[['join_name','osversion_id']]
        location_id_df=self.get_id('view_Location').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'location_id'})[['join_name','location_id']]

        
        ps_coll = self.db['merge_phisical_server']
        ps_df = pd.DataFrame(list(ps_coll.find())).assign(join_brand_name=lambda x:x['merge_brand_name'].str.upper()).assign(join_model_name=lambda x:x['merge_model_name'].str.upper()).assign(join_osfamily_name=lambda x:x['merge_osvendor'].str.upper()).assign(join_osversion_name=lambda x:x['merge_osversion_name'].str.upper()).assign(join_location_name=lambda x:x['merge_location'].str.upper())

        join1 = pd.merge(ps_df,brand_id_df,how='left',left_on='join_brand_name',right_on='join_name')
        join2 = pd.merge(join1,model_id_df,how='left',left_on='join_model_name',right_on='join_name')
        join3 = pd.merge(join2,osfamily_id_df,how='left',left_on='join_osfamily_name',right_on='join_name')
        join4 = pd.merge(join3,osversion_id_df,how='left',left_on='join_osversion_name',right_on='join_name')
        join5 = pd.merge(join4,location_id_df,how='left',left_on='join_location_name',right_on='join_name')
        join6 = join5.assign(org_id=lambda x:1)


        join_prefix = 'join_'
        cols = [ col for col in join6.columns if join_prefix not in col]
        cols.remove('_id')

        merge_prefix = 'merge_'
        col_map = [ col for col in cols if merge_prefix in col]

        col_dict = {}
        for col in col_map:
            col_dict[col]=col.split(merge_prefix)[1]

        ps_src_df = join6[cols].rename(columns=col_dict).rename(columns={'serial_num':'serialnumber','mem_size':'memory'}).drop(['brand_name','model_name','osvendor','osversion_name','location','cpu_thread','maint_from','maint_to'],axis=1)

        # pdb.set_trace()
        
        return ps_src_df

    def main(self):
        ps_source_table = 'synchro_data_physicalserver_78'
        ps_src_df = self.get_ps_src_df()
        self.load_to_itopdb(df=ps_src_df, source_table_name=ps_source_table)
        self.apply_by_php(source_table_name=ps_source_table)

if __name__ == '__main__':
    ps = LoadPS()
    ps.main()

