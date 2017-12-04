from trans_bw import TransBW
from trans_ems import TransEMS
from trans_gfs import TransGFS
from trans_lv import TransLV
from trans_mongo_db import TransMongo
from trans_network import TransNetwork
from trans_nginx import TransNginx
from trans_oc4j import TransOC4J
from trans_ohs import TransOHS
from trans_oracle_db import TransOracle
from trans_physical_server import TransPS
from trans_solr import TransSolr
from trans_spotfire import TransSpotfire
from trans_spotfireweb import TransSpotfireWeb
from trans_storage import TransStorage
from trans_vm import TransVM
from trans_weblogic import TransWLS
from trans_zookeeper import TransZK
from trans_basic import TransBasic
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')
logger.info('start trans cmdb collection')

bw = TransBW()
bw.main()

ems = TransEMS()
ems.main()

gfs = TransGFS()
gfs.main()

lv = TransLV()
lv.main()

mongo = TransMongo()
mongo.main()

network = TransNetwork()
network.main()

nginx = TransNginx()
nginx.main()

oc4j = TransOC4J()
oc4j.main()

ohs = TransOHS()
ohs.main()

oracle = TransOracle()
oracle.main()

ps = TransPS()
ps.main()

solr = TransSolr()
solr.main()

spotfire = TransSpotfire()
spotfire.main()

spotfireweb = TransSpotfireWeb()
spotfireweb.main()

storage = TransStorage()
storage.main()

vm = TransVM()
vm.main()

wls = TransWLS()
wls.main()

zk = TransZK()
zk.main()

tb = TransBasic()
tb.main()

logger.info('end trans cmdb collection')