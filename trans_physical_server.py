import time
import re
from pandas import Series, DataFrame, concat
import pandas as pd
from pymongo import MongoClient
import json
import pdb
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class TransPS():

    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']

    def format_server_name(self,df, col_name):
        df[col_name] = df[col_name].str.lower().map(
            lambda x: x.split('.cargosmart.com')[0])

    def trans_size_to_mb(self,size_str):
        result = re.search(r'(?P<size>\d+)\s*(?P<unit>\w*)',
                           size_str).groupdict()
        g_unit = ['G', 'GB']
        if result.get('unit') in g_unit:
            size_m = str(int(result.get('size')) * 1024)
        else:
            size_m = result.get('size')
        return size_m

    def get_osvendor(self,osversion):
        if 'windows' in osversion.lower().replace(' ', ''):
            osvendor = 'windows'
        elif 'redhat' in osversion.lower().replace(' ', ''):
            osvendor = 'redhat'
        elif 'centos' in osversion.lower().replace(' ', ''):
            osvendor = 'centos'
        elif 'esx' in osversion.lower().replace(' ', ''):
            osvendor = 'esx'
        else:
            osvendor = ''
        return osvendor

    def format_env(self,in_env_name):
        if 'PRE' in in_env_name.upper():
            env_name = 'PP'
        elif 'MAINT' in in_env_name.upper():
            env_name = 'PM'
        else:
            env_name = in_env_name.upper()
        return env_name

    def write_to_cmdb(self,coll_name, df):
        coll = self.db[coll_name]
        result = coll.delete_many({})
        logger.info("%s deleted %s rows" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json.loads(df.to_json(orient='records')))
        logger.info("%s inserted %s rows" %
              (coll_name, str(len(result.inserted_ids))))
       

    def main(self):
        # server
        excel_server_coll = self.db['excel_server']
        vcenter_server_coll = self.db['vcenter_server']
        oem_server_coll = self.db['oem_server']
        vcenter_vm_coll = self.db['vcenter_virtualmachine']
        excel_server_df = pd.DataFrame(list(excel_server_coll.find()))
        vcenter_server_df = pd.DataFrame(list(vcenter_server_coll.find()))
        oem_server_df = pd.DataFrame(list(oem_server_coll.find()))
        vcenter_vm_df = pd.DataFrame(list(vcenter_vm_coll.find()))

        self.format_server_name(excel_server_df, 'excel_name')
        self.format_server_name(vcenter_server_df, 'vc_name')
        self.format_server_name(vcenter_vm_df, 'vc_name')
        self.format_server_name(oem_server_df, 'oem_name')

        # get oem physical server names by (oem servers - vcenter vms)
        tempdf = pd.merge(oem_server_df, vcenter_vm_df,
                          left_on='oem_name', right_on='vc_name', how='left')
        oem_ps_names = tempdf.loc[tempdf['vc_name'].isnull(), 'oem_name']

        # get all physical server by union all excel, vcenter, oem physical server
        # names, and distinct
        excel_ps_names = excel_server_df['excel_name']
        vcenter_ps_names = vcenter_server_df['vc_name']
        ps_names = concat(
            [concat([excel_ps_names, vcenter_ps_names]), oem_ps_names]).unique()

        ps_names_df = pd.DataFrame(ps_names, columns=['ps_name'])
        join1 = pd.merge(ps_names_df, excel_server_df,
                         left_on='ps_name', right_on='excel_name', how='left')
        join2 = pd.merge(join1, vcenter_server_df, left_on='ps_name',
                         right_on='vc_name', how='left')
        ps_df = pd.merge(join2, oem_server_df, left_on='ps_name',
                         right_on='oem_name', how='left').fillna(value='')
        ps_df['merge_name'] = ps_df['ps_name']

        # pdb.set_trace()

        # delete the columns

        delete_cols = [
            mongoid_col for mongoid_col in ps_df.columns if '_id' in mongoid_col]
        delete_cols += ['excel_name', 'vc_name', 'oem_name']
        ps_df = ps_df.drop(delete_cols, axis=1)

        # set cpu num , excel > vc > oem

        ps_df['merge_cpu_num'] = ps_df['vc_cpu_num']
        ps_df.loc[ps_df.merge_cpu_num == '', 'merge_cpu_num'] = ps_df.loc[
            ps_df.merge_cpu_num== '', 'oem_cpu_num']
        ps_df.loc[ps_df.merge_cpu_num == '', 'merge_cpu_num'] = ps_df.loc[
            ps_df.merge_cpu_num == '', 'excel_cpu_num']

        # set cpu type, excel > vc > oem

        ps_df['vc_cpu_type'] = ps_df['vc_cpu_type'].map(lambda x: re.sub(
            r'\s\s+', ' ', str(x).upper().split(' @ ')[0].strip()))

        ps_df['excel_cpu_type'] = ps_df['excel_cpu_type'].map(lambda x: str(x).upper().strip().replace('ULTRASPARC IIII', '  ULTRASPARC-IIII').replace(
            'INTEL(R) XEON®', 'INTEL(R) XEON(R)').replace('INTEL XEON', 'INTEL(R) XEON(R)').replace('INTEL® PENTIUM®', 'INTEL(R) PENTIUM(R)'))

        ps_df['merge_cpu_type'] = ps_df['vc_cpu_type']
        ps_df.loc[ps_df.merge_cpu_type== '', 'merge_cpu_type'] = ps_df.loc[
            ps_df.merge_cpu_type == '', 'excel_cpu_type']


        # set cpu core, excel > vc > oem


        ps_df['merge_cpu_core'] = ps_df['vc_cpu_core']
        ps_df.loc[ps_df['merge_cpu_core'] == '', 'merge_cpu_core'] = ps_df.loc[
            ps_df['merge_cpu_core'] == '', 'oem_cpu_num']
        ps_df.loc[ps_df['merge_cpu_core'] == '', 'merge_cpu_core'] = ps_df.loc[
            ps_df['merge_cpu_core'] == '', 'excel_cpu_core']

        # set cpu speed, excel > vc > oem

        ps_df['merge_cpu_speed'] = ps_df["vc_cpu_speedGHz"]

        ps_df['merge_cpu_cache_size'] = ps_df['excel_cpu_cache_size']

        ps_df['merge_cpu_thread'] = ps_df['vc_cpu_thread']        

        # set memory size, priority: vc > oem > excel

        ps_df.loc[ps_df['excel_memory_size'] != '', 'excel_memory_size'] = ps_df.loc[ps_df[
            'excel_memory_size'] != '', 'excel_memory_size'].map(lambda x: self.trans_size_to_mb(x))

        ps_df['merge_mem_size'] = ps_df["vc_memory_size"]

        ps_df.loc[ps_df.merge_mem_size== '', 'merge_mem_size'] = ps_df.loc[
            ps_df.merge_mem_size == '', 'oem_memory_size']

        ps_df.loc[ps_df['merge_mem_size'] == '', 'merge_mem_size'] = ps_df.loc[
            ps_df['merge_mem_size'] == '', 'excel_memory_size']

        # set system disk, excel
        ps_df['merge_system_disk'] = ps_df['excel_system_disk']
        ps_df['merge_external_disk'] = ps_df[
            'excel_external_disk'].str.replace('Nil', '')

        # set brand name, excel
        ps_df['merge_brand_name'] = ps_df['vc_brand_name']

        # set model , excel
        ps_df['merge_model_name'] = ps_df["vc_model_name"]

        # pdb.set_trace()

        # set os version , excel > oem
        ps_df['merge_osversion_name'] = ps_df["vc_os_version"]
        ps_df.loc[ps_df['merge_osversion_name'] == '', 'merge_osversion_name'] = ps_df.loc[
            ps_df['merge_osversion_name'] == '', 'oem_osversion_name']


        # set os vendor
        ps_df['merge_osvendor'] = ps_df['merge_osversion_name'].map(lambda x: self.get_osvendor(x))

        # set environment by excel

        ps_df['merge_env_purpose'] = ps_df[
            'excel_env_purpose'].map(lambda x: self.format_env(x))

        ps_df['merge_environment'] = ps_df['vc_env']

        # set others by excel

        # ps_df['merge_fiber_card2_model'] = ps_df['excel_fiber_card2_model']
        # ps_df['merge_fiber_card3_model'] = ps_df[
        #     'excel_fiber_card3_model'].str.replace(' ', '')
        # ps_df['merge_fiber_card_model'] = ps_df[
            # 'excel_fiber_card_model'].str.upper().replace('NIL', '')

        ps_df['merge_fiber_card_model'] = ps_df['vc_fiber_hba_device']
        # ps_df['merge_fiber_card_num'] = ps_df[
        #     'excel_fiber_card_num'].str.replace(' ', '').fillna('')
        ps_df['merge_fiber_card_num'] = ps_df['vc_fiber_hba_num']

        ps_df['merge_fiber_port'] = ps_df['excel_fiber_port']
        ps_df.loc[ps_df['excel_fiber_port'] != '', 'excel_fiber_port'] = ps_df.loc[
            ps_df['excel_fiber_port'] != '', 'excel_fiber_port'].map(lambda x: str(int(x)))

        ps_df['merge_hw_model_eol_date'] = ps_df['excel_hw_model_eol_date']

        ps_df['merge_ip'] = ps_df['vc_ip']

        ps_df['merge_lan_port'] = ps_df['excel_lan_port']

        ps_df.loc[ps_df['excel_lan_port'] != '', 'excel_lan_port'] = ps_df.loc[
            ps_df['excel_lan_port'] != '', 'excel_lan_port'].map(lambda x: str(int(x)))


        ps_df['merge_location'] = ps_df['excel_location']

        ps_df['merge_maint_from'] = ps_df[
            'excel_maint_from'].map(lambda x: x.strip())

        ps_df['merge_maint_status'] = ps_df['excel_maint_status']

        ps_df['merge_maint_to'] = ps_df['excel_maint_to']

        ps_df['merge_maint_vendor'] = ps_df['excel_maint_vendor']
        
        ps_df['merge_os_service_pack'] = ps_df[
            'excel_os_service_pack'].map(lambda x: str(x))

        ps_df['merge_power_port'] = ps_df['excel_power_port']
        ps_df.loc[ps_df['merge_power_port'] != '', 'merge_power_port'] = ps_df.loc[
            ps_df['merge_power_port'] != '', 'merge_power_port'].map(lambda x: str(int(x)))

        ps_df['merge_power_status'] = ps_df['vc_power_status'].str.upper()

        ps_df['merge_rack_location'] = ps_df['excel_rack_location'].map(
            lambda x: x.upper().replace(' ', ''))

        ps_df['merge_serial_num'] = ps_df['excel_serial_num']
        ps_df['merge_server_function'] = ps_df['excel_server_function']

        ps_df['merge_server_type'] = ps_df['excel_server_type']

        ps_df['merge_check_by'] = ps_df['excel_check_by']

        ps_df['merge_check_date'] = ps_df['last on-site check date']

        merge_cols = [col.lower()
                      for col in ps_df.columns if 'merge' in col.lower()]

        ps_df = ps_df[merge_cols]

        # write to mongodb
        self.write_to_cmdb(coll_name='merge_phisical_server', df=ps_df)
        self.client.close()

if __name__ == '__main__':
    ps = TransPS()
    ps.main()

