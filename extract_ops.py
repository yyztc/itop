from requests.auth import HTTPDigestAuth
import requests
from pprint import pprint
from pymongo import MongoClient
import logging

logging.basicConfig(
    level=logging.INFO,
    #level=logging.WARNING,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s",
)


class ExtractOPS():

    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']

    def get_format_data(self, db_obj, key_name):
        value = str(db_obj.get(key_name, '')).upper()
        if key_name == 'hostname':
            value = value.split('.CARGOSMART.COM')[0]
        return value

    def get_db_list(self, url, username, nonce):
        result = requests.get(url, auth=HTTPDigestAuth(
            username, nonce)).json()['results']
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
        ops_config={}
        ops_config['user']='cshs@cargosmart.com'
        ops_config['nonce']='931297d2-4ccd-4c01-8455-e065c9622a1f'
        ops_config['url']='http://fm_mongoarb:8080/api/public/v1.0/groups/55d1ac57e4b0da06286ea857/hosts'
    
        url = ops_config['url']
        username = ops_config['user']
        nonce = ops_config['nonce']
        db_list = self.get_db_list(url, username, nonce)
        self.load_jsonlist_to_mongodb(coll_name='ops_database',json_list=db_list)
        self.client.close()

if __name__ == '__main__':
    ops = ExtractOPS()
    ops.main()
