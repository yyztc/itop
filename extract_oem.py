import cx_Oracle
from pymongo import MongoClient
from pprint import pprint
import logging

logging.basicConfig(
    level=logging.INFO,
    #level=logging.WARNING,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s",
)

class ExtractOEM11G():

    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']

    def get_connect(self, host, port, dbname, user, passwd):
        connect_str = '%s:%s/%s' % (host, port, dbname)
        self.connect = cx_Oracle.connect(user, passwd, connect_str)

    def get_query_result(self, sql):
        cursor = self.connect.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def close_connect(self):
        self.connect.close()

    def get_server_list(self):
        sql_get_server = 'select HOST_NAME, DOMAIN, OS_SUMMARY, MEM, DISK, CPU_COUNT, OS_VENDOR from mgmt$os_hw_summary'
        result = self.get_query_result(sql_get_server)
        server_list = []

        for rs in result:
            server = {}
            server['oem_name'] = rs[0]
            server['oem_domain'] = rs[1]
            server['oem_osversion_name'] = rs[2]
            server['oem_memory_size'] = rs[3]
            server['oem_system_disk'] = rs[4]
            server['oem_cpu_num'] = 2
            # server['oem_cpu_num'] = rs[5]
            server['oem_os_vendor'] = rs[6]
            server['oem_environment'] = 'PROD'
            server_list.append(server)
        return server_list

    def get_db_list(self):
        sql_get_db = "select TARGET_NAME, HOST_NAME, HOME_NAME, HOME_LOCATION,  COMPONENT_EXTERNAL_NAME, COMPONENT_VERSION from MGMT$TARGET_COMPONENTS where target_type='oracle_database' order by TARGET_NAME"
        result = self.get_query_result(sql_get_db)

        db_list = []
        for rs in result:
            db = {}
            db['oem_name'] = rs[0]
            db['oem_host_name'] = rs[1]
            db['oem_home_name'] = rs[2]
            db['oem_home_location'] = rs[3]
            db['oem_software'] = rs[4]
            db['oem_version'] = rs[5]
            db['oem_environment'] = 'PROD'
            db_list.append(db)
        return db_list

    def load_jsonlist_to_mongodb(self, coll_name, json_list):
        coll = self.db[coll_name]
        # result = coll.delete_many({})
        # print("%s deleted %s" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json_list)
        logging.info("%s inserted %s" % (coll_name, str(len(result.inserted_ids))))

    def main(self):
        user = 'cs2supp'
        passwd = 'Password1'
        host = 'oem02'
        port = '1521'
        dbname = 'emrep'

        self.get_connect(host, port, dbname, user, passwd)
        server_list = self.get_server_list()
        db_list = self.get_db_list()
        self.close_connect()
        self.load_jsonlist_to_mongodb(
            coll_name='oem_server', json_list=server_list)
        self.load_jsonlist_to_mongodb(
            coll_name='oem_database', json_list=db_list)
        self.client.close()

if __name__ == '__main__':
    oem = ExtractOEM11G()
    oem.main()
