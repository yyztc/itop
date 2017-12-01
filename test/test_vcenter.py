from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import atexit
from pprint import pprint

context = None
if hasattr(ssl, '_create_unverified_context'):
    context = ssl._create_unverified_context()
connect = SmartConnect(host='vc06', user='cssupp',
                       pwd='cs2pwd', port=443, sslContext=context)
if not connect:
    print("Could not connect to the specified host using specified "
          "username and password")
    raise IOError
else:
    pass
atexit.register(Disconnect, connect)

content = connect.RetrieveContent()

dc = content.rootFolder.childEntity[0]

cluster = dc.hostFolder.childEntity[0]

host = cluster.host[0]