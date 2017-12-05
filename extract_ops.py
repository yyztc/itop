from requests.auth import HTTPDigestAuth
import requests
from pprint import pprint
from pymongo import MongoClient
import logging
import configparser

logging.basicConfig(
    level=logging.INFO,
    #level=logging.WARNING,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s",
)


class ExtractOPS():

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

    def get_format_data(self, db_obj, key_name):
        value = str(db_obj.get(key_name, '')).upper()
        if key_name == 'hostname':
            value = value.split('.CARGOSMART.COM')[0]
        return value

    def get_db_list(self, url, username, nonce):
        resp = requests.get(url, auth=HTTPDigestAuth(username, nonce))
        # logging.info(resp)
        result = resp.json()['results']
        # logging.info(result)
        # result = requests.get(url, auth=HTTPDigestAuth(username, nonce)).json()['results']
        db_list = []
        for db_obj in result:
            db = {}
            db['ops_port'] = db_obj.get('port','')
            db['ops_hostname'] = db_obj.get('hostname','')
            db['ops_name'] = db['ops_hostname'] + ':' +str(db['ops_port'])
            db['ops_authMechanismName'] = db_obj.get('authMechanismName','')
            db['ops_created'] = db_obj.get('created','')
            db['ops_hidden'] = db_obj.get('hidden','')
            db['ops_journalingEnabled'] = db_obj.get('journalingEnabled','')
            db['ops_logsEnabled'] = db_obj.get('logsEnabled','')
            db['ops_profilerEnabled'] = db_obj.get('profilerEnabled','')
            db['ops_typeName'] = db_obj.get('typeName','')
            db['ops_sslEnabled'] = db_obj.get('sslEnabled','')
            db['ops_replicaSetName'] = db_obj.get('replicaSetName','')
            db['ops_replicaStateName'] = db_obj.get('replicaStateName','')
            db['ops_version'] = db_obj.get('version','')
            db_list.append(db)
        return db_list

    def load_jsonlist_to_mongodb(self,coll_name, json_list):
        coll = self.db[coll_name]
        # result = coll.delete_many({})
        # print("%s deleted %s" % (coll_name, str(result.deleted_count)))
        result = coll.insert_many(json_list)
        logging.info("%s inserted %s" % (coll_name, str(len(result.inserted_ids))))
    
    def main(self):
        section = "ops"
        ops_user = self.cfg.get(section,"user")
        ops_nonce = self.cfg.get(section,"nonce")
        ops_url = self.cfg.get(section,"url")
    
        db_list = self.get_db_list(url=ops_url, username=ops_user, nonce=ops_nonce)
        self.load_jsonlist_to_mongodb(coll_name='ops_database',json_list=db_list)
        self.client.close()

if __name__ == '__main__':
    ops = ExtractOPS()
    ops.main()
