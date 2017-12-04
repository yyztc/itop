import urllib.request
import urllib.parse
import json 
from pprint import pprint

def http_post(opr): 
    url="http://ppitop01/itop/webservices/rest.php?version=1.3" 
    auth ={"auth_user": "admin" , "auth_pwd": "Password1"}
    oprjson =urllib.parse.urlencode({'json_data': json.dumps(opr)})
    jdata = urllib.parse.urlencode(auth)
    jdata = jdata+'&'+oprjson
    response = urllib.request.urlopen(url,jdata.encode(encoding='UTF8'))
    return response.read()         

class_list = [
'lnkVirtualDeviceToVolume',
'lnkServerToVolume',
'LogicalVolume',
'MongoDBInstance',
'DatabaseSchema',
'DBServer',
'NginxInstance',
'OHSInstance',
'SpotfireInstance',
'SpotfireWebInstance',
'BWInstance',
'EMSInstance',
'OC4JInstance',
'SolrInstance',
'WeblogicInstance',
'GFSInstance',
'ZooKeeperInstance',
'WebServer',
'MiddlewareInstance',
'Middleware',
'StorageSystem',
'VirtualMachine',
'Hypervisor',
'Server',
'PhysicalServer',
'NetworkDevice',
'NetworkDeviceType',
'LogicalVolume',
'OSVersion',
'OSFamily',
'Model',
'Brand',
'ApplicationSolution'
]

for class_name in class_list:
    key_name = "SELECT {class_name}".format(class_name=class_name)
    opr={
        "operation": "core/delete",
        "comment": "Cleanup for customer Demo",
        "class": "{class_name}".format(class_name=class_name),
        "key":"{key}".format(key=key_name), 
        "simulate": False
    }

    # pprint(opr)
    rep=http_post(opr)
    pprint(rep)
    pprint("deleted {class_name}".format(class_name=class_name))