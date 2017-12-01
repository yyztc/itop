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

class LoadVMwareLicense():

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

    def get_vmware_license_src_df(self):
        vmware_license_coll = self.db['merge_vmware_license']
        vmware_license_df = pd.DataFrame(list(vmware_license_coll.find()))

        prefix = 'merge_'
        col_dict = {}
        for col in vmware_license_df.columns:
            if prefix in col:
                col_dict[col] = col.split(prefix)[1]

        # logger.info(vmware_license_df.rename(columns=col_dict))

        vmware_license_src_df = vmware_license_df.rename(columns=col_dict).assign(org_id=lambda x:1).assign(primary_key=lambda x:x['license_key']).assign(perpetual=lambda x:'yes').assign(licence_key=lambda x:x['license_key']).assign(software_id=lambda x:1)[['name','org_id','usage_limit','licence_key','perpetual','environment','used','primary_key','software_id']]

        return vmware_license_src_df

    def main(self):
        vmware_license_src_df = self.get_vmware_license_src_df()
        self.load_to_itopdb(
            df=vmware_license_src_df, source_table_name='synchro_data_vmwarelicense_103')
        self.apply_by_php(source_table_name='synchro_data_vmwarelicense_103')


if __name__ == '__main__':
    license = LoadVMwareLicense()
    license.main()
