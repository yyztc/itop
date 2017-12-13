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

class LoadStorage():

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

    def get_st_src_df(self):
        brand_id_df=self.get_id('view_Brand').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'brand_id'})[['join_name','brand_id']]
        model_id_df=self.get_id('view_Model').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'model_id'})[['join_name','model_id']]
        location_id_df=self.get_id('view_Location').assign(join_name=lambda x:x['name'].str.upper()).rename(columns={'id':'location_id'})[['join_name','location_id']]

        
        st_coll = self.db['merge_storage']
        st_df = pd.DataFrame(list(st_coll.find())).assign(join_brand_name=lambda x:x['merge_vendor'].str.upper()).assign(join_model_name=lambda x:x['merge_model_name'].str.upper()).assign(join_location_name=lambda x:x['merge_location'].str.upper())

        join1 = pd.merge(st_df,brand_id_df,how='left',left_on='join_brand_name',right_on='join_name')
        join2 = pd.merge(join1,model_id_df,how='left',left_on='join_model_name',right_on='join_name')
        join3 = pd.merge(join2,location_id_df,how='left',left_on='join_location_name',right_on='join_name')
        join4 = join3.assign(org_id=lambda x:1)

        join_prefix = 'join_'
        cols = [ col for col in join4.columns if join_prefix not in col]
        cols.remove('_id')

        merge_prefix = 'merge_'
        col_map = [ col for col in cols if merge_prefix in col]

        col_dict = {}
        for col in col_map:
            col_dict[col]=col.split(merge_prefix)[1]

        st_src_df = join4[cols].rename(columns=col_dict).rename(columns={'serial_num':'serialnumber'})
        st_src_df['serialnumber'] =st_src_df['serialnumber'].map(lambda x:str(x))
        st_src_df['name'] = st_src_df['model_name'].values + ':'+ st_src_df['serialnumber'].values
        st_src_df['description'] = st_src_df['function']
        st_src_df['primary_key'] = st_src_df['name']
        # logger.info(st_src_df)
        st_src_df = st_src_df[['primary_key','name','serialnumber','org_id','location_id','brand_id','model_id','description']]
        # st_src_df = st_src_df[['primary_key','name','serialnumber','org_id','location_id','brand_id','model_id','description','function','check_date','maint_vendor','maint_period','power_status','rack_location','remarks']]

        return st_src_df

    def main(self):
        st_source_table = 'synchro_data_storagesystem_79'
        st_src_df = self.get_st_src_df()
        # logger.info(st_src_df.columns)
        # logger.info(st_src_df.head(10))
        self.load_to_itopdb(df=st_src_df, source_table_name=st_source_table)
        self.apply_by_php(source_table_name=st_source_table)

if __name__ == '__main__':
    st = LoadStorage()
    st.main()

