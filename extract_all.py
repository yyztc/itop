from extract_excel import ExtractExcel
from extract_oem import ExtractOEM11G
from extract_ops import ExtractOPS
from extract_vcenter import ExtractVcenter
from extract_zabbix import ExtractZabbix
from pymongo import MongoClient
import logging
from logging.config import fileConfig
import configparser

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')

class ExtractAll():

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

    def truncate_cmdb(self, coll_name):
        coll = self.db[coll_name]
        result = coll.delete_many({})
        logging.info("%s deleted %s" % (coll_name, str(result.deleted_count)))


    def main(self):
        coll_list = ['excel_server','excel_storage','excel_network','oem_server','oem_database','ops_database','vcenter_server','vcenter_virtualmachine','vcenter_logicalvolume','zabbix_weblogic','zabbix_oc4j','zabbix_solr','zabbix_bw','zabbix_ems','zabbix_nginx','zabbix_ohs','zabbix_spotfirewebplayer','zabbix_spotfire','zabbix_gfs','zabbix_zookeeper','zabbix_others']

        logger.info('start clear cmdb collections')
        for coll in coll_list:
            self.truncate_cmdb(coll)
        logger.info('end clear cmdb collections')

        logger.info('start extract cmdb collections')
        ex = ExtractExcel()
        ex.main()
        oem = ExtractOEM11G()
        oem.main()
        ops = ExtractOPS()
        ops.main()
        vc = ExtractVcenter()
        vc.main()
        zb = ExtractZabbix()
        zb.main()
        logger.info('end extract cmdb collections')

if __name__ == '__main__':
    ext = ExtractAll()
    ext.main()

