import re
from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransVMwareLicense():

    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']

    def write_to_cmdb(self, coll_name, df):
        coll = self.db[coll_name]
        result = coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" %
              (coll_name, str(len(result.inserted_ids))))

    def main(self):
        vmware_license_coll = self.db['vcenter06_vmware_license']
        vmware_license_df = pd.DataFrame(
            list(vmware_license_coll.find())).fillna(value='')
        merge_vmware_license_df = pd.DataFrame()
        merge_vmware_license_df['merge_environment'] = vmware_license_df[
            'environment']
        merge_vmware_license_df['merge_usage_limit'] = vmware_license_df[
            'total'].map(lambda x:str(x)).values + ' ' + vmware_license_df['costUnit'].values
        merge_vmware_license_df['merge_used'] = vmware_license_df['used'].map(lambda x:str(x)) + ' ' +vmware_license_df['costUnit'].values
        merge_vmware_license_df['merge_name'] = vmware_license_df['name']
        merge_vmware_license_df['merge_license_key'] = vmware_license_df['licenseKey']
        self.write_to_cmdb(coll_name='merge_vmware_license', df=merge_vmware_license_df)
        self.client.close()

if __name__ == '__main__':
    license = TransVMwareLicense()
    license.main()
