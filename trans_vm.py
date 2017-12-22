
import re
from pandas import Series,DataFrame,concat
import pandas as pd
from pymongo import MongoClient
import json
import logging
from logging.config import fileConfig
import configparser

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransVM():
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read("config.ini")        
        cmdb_db = self.cfg.get("cmdb","db")
        cmdb_str = self.cfg.get("cmdb","conn_str")
        self.client = MongoClient(cmdb_str)
        self.db = self.client[cmdb_db]

    def write_to_cmdb(self,coll_name,df):
        coll = self.db[coll_name]
        result =  coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name,str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" % (coll_name,str(len(result.inserted_ids))))

    def main(self):
        # vm 
        vc_vm_coll = self.db['vcenter_virtualmachine']
        vc_vm_df = pd.DataFrame(list(vc_vm_coll.find())).fillna(value='')
        
        vm_df = pd.DataFrame()
        vm_df['merge_cpu_num'] = vc_vm_df['vc_cpu_num']
        vm_df['merge_ethernetcard_num'] = vc_vm_df['vc_ethernetcard_num']
        vm_df['merge_ip'] = vc_vm_df['vc_ip']
        vm_df['merge_mem_size'] = vc_vm_df['vc_memory_size'].map(lambda x:str(x))
        vm_df['merge_name'] = vc_vm_df['vc_name']
        vm_df['merge_power_status'] = vc_vm_df['vc_power_status'].str.upper()
        vm_df['merge_osversion'] = vc_vm_df['vc_server_fullname']
        
        def get_osvendor(osversion):
            if 'windows' in osversion.lower().replace(' ',''):
                osvendor = 'windows'
            elif 'redhat' in osversion.lower().replace(' ',''):
                osvendor = 'redhat'
            elif 'centos' in osversion.lower().replace(' ',''):
                osvendor = 'centos'
            elif 'esx' in osversion.lower().replace(' ',''):
                osvendor = 'esx'
            elif 'suse' in osversion.lower().replace(' ',''):
                osvendor = 'suse'
            elif 'ubuntu' in osversion.lower().replace(' ',''):
                osvendor = 'ubuntu'
            else:
                osvendor = ''
            return osvendor
        
        vm_df['merge_osfamily'] = vc_vm_df['vc_server_fullname'].map(lambda x:get_osvendor(x))
        vm_df['merge_physical_server'] = vc_vm_df['vc_server_name'].map(lambda x:x.lower().split('.cargosmart.com')[0])
        vm_df['merge_lun_name'] = vc_vm_df['vc_vm_path'].map(lambda x:re.search(r'\[(?P<lun_name>.*)\]\s+(?P<vmx_path>.*)',x).      groupdict().get('lun_name'))
        vm_df['merge_vmx_path'] = vc_vm_df['vc_vm_path'].map(lambda x:re.search(r'\[(?P<lun_name>.*)\]\s+(?P<vmx_path>.*)',x).      groupdict().get('vmx_path'))
        vm_df['merge_annotation'] = vc_vm_df['vc_annotation']
        
        self.write_to_cmdb(coll_name='merge_virtualmachine',df=vm_df)
        self.client.close()

if __name__ == '__main__':
    vm = TransVM()
    vm.main()