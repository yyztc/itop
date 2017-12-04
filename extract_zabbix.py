from pymongo import MongoClient
from pprint import pprint
from pyzabbix import ZabbixAPI
from pprint import pprint
import logging
import configparser

logging.basicConfig(
    level=logging.INFO,
    #level=logging.WARNING,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s",
)


class ExtractZabbix():

    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read("config.ini")
        
        cmdb_host = self.cfg.get("cmdb","host")
        cmdb_port = self.cfg.get("cmdb","port")
        cmdb_user = self.cfg.get("cmdb","user")
        cmdb_passwd = self.cfg.get("cmdb","passwd")
        cmdb_db = self.cfg.get("cmdb","db")
        cmdb_str = "mongodb://%s:%s@%s:%s" % (cmdb_user,cmdb_passwd,cmdb_host,cmdb_port)
        self.client = MongoClient(cmdb_str)
        self.db = self.client[cmdb_db]

        zabbix_url = self.cfg.get("zabbix","url")
        zabbix_user = self.cfg.get("zabbix","user")
        zabbix_passwd = self.cfg.get("zabbix","passwd")

        self.zb = ZabbixAPI(zabbix_url)
        self.zb.login(zabbix_user, zabbix_passwd)

    def get_items_by_app(self, application_name):
        items = self.zb.item.get(application=application_name, output=[
            "name", "key_", "prevvalue", "hostid"], selectHosts=['host'])
        return items

    def add_property(self, items):
        for item in items:
            item.setdefault('environment', 'PROD')
        return items

    def load_jsonlist_to_mongodb(self, coll_name, json_list):
        coll = self.db[coll_name]
        # result = coll.delete_many({})
        # print("%s deleted %s" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json_list)
        logging.info("%s inserted %s" % (coll_name, str(len(result.inserted_ids))))

    def get_project_servers(self):
        project_servers = self.zb.hostgroup.get(
            search={'name': 'Project'}, selectHosts=['host'])
        return project_servers

    def load_items_to_mongodb(self, application_name=None):
        if 'wls' in application_name.lower():
            collection_name = 'zabbix_weblogic'
        elif 'oc4j' in application_name.lower():
            collection_name = 'zabbix_oc4j'
        elif 'solr' in application_name.lower():
            collection_name = 'zabbix_solr'
        elif 'bw' in application_name.lower():
            collection_name = 'zabbix_bw'
        elif 'ems' in application_name.lower():
            collection_name = 'zabbix_ems'
        elif 'nginx' in application_name.lower():
            collection_name = 'zabbix_nginx'
        elif 'ohs' in application_name.lower():
            collection_name = 'zabbix_ohs'
        elif 'spotfirewebplayer' in application_name.lower():
            collection_name = 'zabbix_spotfirewebplayer'
        elif 'spotfire' in application_name.lower():
            collection_name = 'zabbix_spotfire'
        elif 'gfs' in application_name.lower():
            collection_name = 'zabbix_gfs'
        elif 'zookeeper' in application_name.lower():
            collection_name = 'zabbix_zookeeper'
        else:
            collection_name = 'zabbix_others'

        items = self.get_items_by_app(application_name=application_name)
        items = self.add_property(items)

        json_list = []
        for it in items:
            temp_json = {}
            for k, v in it.items():
                temp_json['zabbix_' + k] = v
            json_list.append(temp_json)

        self.load_jsonlist_to_mongodb(
            coll_name=collection_name, json_list=json_list)

    def load_project_servers_to_mongodb(self):

        project_servers = self.get_project_servers()
        project_servers = self.add_property(project_servers)

        json_list = []
        for it in project_servers:
            temp_json = {}
            for k, v in it.items():
                temp_json['zabbix_' + k] = v
            json_list.append(temp_json)
        self.load_jsonlist_to_mongodb(
            coll_name='zabbix_project_servers', json_list=json_list)

    def main(self):
        application_names = ['WLS_Process_Status', 'WLS_Metrics', 'OC4J_Process_Status', 'OC4J_Metrics', 'Solr_Process_Status', 'Solr_System_Metrics', 'BW_Process_Status', 'EMS_Process_Status',
                             'Nginx_Process_Status', 'OHS_Process_Status', 'SpotfireServer_Process_Status', 'SpotfireWebPlayer_Process_Status', 'GFS_Process_Status', 'Zookeeper_Process_Status']
        for application_name in application_names:
            self.load_items_to_mongodb(application_name=application_name)
        self.load_project_servers_to_mongodb()
        self.client.close()


if __name__ == '__main__':
    zb = ExtractZabbix()
    zb.main()
