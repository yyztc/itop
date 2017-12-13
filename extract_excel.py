import re
import numpy as np
import pandas as pd
from pprint import pprint
import json
from pymongo import MongoClient
import datetime
import logging
import configparser

logging.basicConfig(
    level=logging.INFO,
    #level=logging.WARNING,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s",
)


class ExtractExcel():

    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read("config.ini")        
        cmdb_db = self.cfg.get("cmdb","db")
        cmdb_str = self.cfg.get("cmdb","conn_str")
        self.client = MongoClient(cmdb_str)
        self.db = self.client[cmdb_db]

        self.excel_filename = self.cfg.get("excel","filename")

    def _write_to_cmdb(self, coll_name, json_data):
        coll = self.db[coll_name]
        # result = coll.delete_many({})
        # print("%s deleted %s" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json.loads(json_data))
        logging.info("%s inserted %s" % (coll_name, str(len(result.inserted_ids))))

    def _add_col_prefix(self, label_dict, label_prefix='excel_'):
        for k, v in label_dict.items():
            label_dict[k] = label_prefix + v

    def _format_datetime(self,datetime_str):
        if datetime_str:
            (y,m,d)=datetime_str.split()[0].split('-')
            res =  datetime.datetime(int(y),int(m),int(d)).strftime('%b %d %Y %H:%M:%S')
        else:
            res = datetime_str
        return res


    def extract_physical_server(self):
        ps_map_dict = {'site': 'location', 'location': 'rack_location', 'purpose of environment': 'env_purpose', 'type of environment': 'environment', 'server name': 'name', 'esx/non-esx': 'server_type', 'power on/ power off': 'power_status', 'operation system': 'osversion_name', 'os service pack': 'os_service_pack',  'server function': 'server_function',
                       'manufacture brand(hp/dell/sun)': 'brand_name', 'server model': 'model_name', 'cpu type': 'cpu_type', 'cpu speed': 'cpu_speed', '# of cpu': 'cpu_num', '# of core': 'cpu_core', 'cpu cache size': 'cpu_cache_size', 'memory': 'memory_size', 'system disk': 'system_disk', 'external disk': 'external_disk', 'ip address': 'ip', 'power port': 'power_port', 'lan port': 'lan_port', 'fiber port': 'fiber_port', 'fiber card model': 'fiber_card_model', 'total of fiber card': 'fiber_card_num', 'fiber card2 model': 'fiber_card2_model', 'fiber card3 model': 'fiber_card3_model', 'serial #': 'serial_num', 'maint. status': 'maint_status', 'maint. service from': 'maint_from', 'maint. service to': 'maint_to', 'maint vendor': 'maint_vendor', 'hw model eol date': 'hw_model_eol_date', 'last on-site   check date': 'check_date', 'last on-site check by': 'check_by'}

        self._add_col_prefix(label_dict=ps_map_dict)


        excel_ps_df = pd.DataFrame(pd.read_excel(self.excel_filename, 'Physical Server')).rename(columns=str.lower). rename(columns=ps_map_dict).drop(
            'project', axis=1).assign(excel_name=lambda x: x['excel_name'].map(lambda x: str(x).lower()))

        excel_ps_df['last on-site check date'] = excel_ps_df['last on-site check date'].map(lambda x:self._format_datetime(str(x)))

        excel_ps_df=excel_ps_df.loc[excel_ps_df.excel_power_status.str.upper()=='POWER ON']

        excel_ps_json = excel_ps_df.to_json(orient='records')

        self._write_to_cmdb(coll_name='excel_server', json_data=excel_ps_json)

    def extract_storage(self):
        st_map_dict = {
            'site': 'location',
            'location': 'rack_location',
            'hardware model': 'model_name',
            'serial number': 'serial_num',
            'hardware type': 'type',
            'power staus': 'power_status',
            'maint period': 'maint_period',
            'maint vendor': 'maint_vendor',
            'on-site check date': 'check_date',
            'on-site check by': 'check_by',
            'vendor': 'vendor',
            'remarks': 'remarks',
            'function': 'function'
        }

        self._add_col_prefix(label_dict=st_map_dict)

        excel_st_json = pd.DataFrame(pd.read_excel(self.excel_filename, 'Storage related')).rename(
            columns=str.lower).rename(columns=st_map_dict).to_json(orient='records')

        self._write_to_cmdb(coll_name='excel_storage', json_data=excel_st_json)

    def extract_network(self):
        network_map_dict = {
            'site': 'location',
            'location': 'rack_location',
            'hostname': 'hostname',
            'ip address': 'ip',
            'model': 'model_name',
            'serial no.': 'serial_num',
            'brand': 'brand_name',
            'maintenance\nrequired': 'maint_required',
            'eol date': 'eol_date',
            'status': 'use_status',
            'remarks': 'remarks',
            'on-site check date': 'check_date',
            'on-site check by': 'check_by'
        }

        self._add_col_prefix(label_dict=network_map_dict)

        excel_network_json = pd.DataFrame(pd.read_excel(self.excel_filename, 'Network & KVM SW & UPS')).rename(
            columns=str.lower).rename(columns=network_map_dict).to_json(orient='records')
        self._write_to_cmdb(coll_name='excel_network',
                            json_data=excel_network_json)

    def main(self):
        self.extract_physical_server()
        self.extract_storage()
        self.extract_network()
        self.client.close()

if __name__ == '__main__':
    ex = ExtractExcel()
    ex.main()
