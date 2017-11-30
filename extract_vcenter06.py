from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import atexit
from pprint import pprint
import json
from pymongo import MongoClient

class ExtractVcenter():
    def __init__(self):
        self.client = MongoClient('mongodb://dba:dba@127.0.0.1:27017')
        self.db = self.client['cmdb']

    def get_connect(self, in_host, in_user, in_pwd, in_port):
        context = None
        if hasattr(ssl, '_create_unverified_context'):
            context = ssl._create_unverified_context()
        self.in_host = in_host
        self.connect = SmartConnect(host=in_host, user=in_user,
                               pwd=in_pwd, port=in_port, sslContext=context)
        if not self.connect:
            print("Could not connect to the specified host using specified "
                  "username and password")
            raise IOError
        else:
            print("connected")
        atexit.register(Disconnect, self.connect)


    def get_license_list(self):
        content = self.connect.RetrieveContent()
        
        license_list = []
        for it in content.licenseManager.licenses:
            if it.name != 'Product Evaluation':
                license = {}
                license['costUnit'] = it.costUnit
                license['editionKey'] = it.editionKey
                license['labels'] = it.labels
                license['licenseKey'] = it.licenseKey
                license['name'] = it.name
                license['total'] = it.total
                license['used'] = it.used
                if 'PP' in self.in_host.upper():
                    license['environment'] = 'NON-PROD'
                else:
                    license['environment'] = 'PROD'
                license_list.append(license)
        return license_list
        
    def load_jsonlist_to_mongodb(self, coll_name, json_list):
        coll = self.db[coll_name]
        result = coll.insert_many(json_list)
        print("%s inserted %s" % (coll_name, str(len(result.inserted_ids))))

    def main(self):
        connect = self.get_connect('ppvc06', 'cssupp', 'Cs2pwd', 443)
        license_list = self.get_license_list()
        self.load_jsonlist_to_mongodb(coll_name='vcenter06_vmware_license', json_list=license_list)

        connect = self.get_connect('vc06', 'cssupp', 'cs2pwd', 443)
        license_list = self.get_license_list()
        self.load_jsonlist_to_mongodb(coll_name='vcenter06_vmware_license', json_list=license_list)

if __name__ == '__main__':
    vc = ExtractVcenter()
    vc.main()
