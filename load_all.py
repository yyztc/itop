from load_appsolution import LoadAppsolution
from load_zookeeper import LoadZookeeper
from load_gfs import LoadGFS
from load_spotfire import LoadSpotfire
from load_weblogic import LoadWLS
from load_solr import LoadSolr
from load_oc4j import LoadOC4J
from load_basic import LoadBasic
from load_bw import LoadBW
from load_dbserver import LoadDBServer
from load_ems import LoadEMS
from load_hypervisor import LoadHyper
from load_middleware import LoadMiddleware
from load_mongo_db import LoadMongoDB
from load_network import LoadNetwork
from load_nginx import LoadNginx
from load_ohs import LoadOHS
from load_oracle_db import LoadOracleDB
from load_physical_server import LoadPS
from load_spotfireweb import LoadSpotfireWeb
from load_storage import LoadStorage
from load_webserver import LoadWebServer
from load_link_appsolution import LoadLinkAppsolution
from load_virtualmachine import LoadVM
import logging
from logging.config import fileConfig

fileConfig('logger_config.ini')
logger=logging.getLogger('infoLogger')


load_order_list = [
LoadBasic,
LoadNetwork,
LoadPS,
LoadHyper,
LoadVM,
LoadStorage,
LoadMiddleware,
LoadWebServer,
LoadZookeeper,
LoadGFS,
LoadWLS,
LoadSolr,
LoadOC4J,
LoadBW,
LoadSpotfire,
LoadSpotfireWeb,
LoadOHS,
LoadNginx,
LoadEMS,
LoadDBServer,
LoadMongoDB,
LoadOracleDB,
LoadAppsolution,
LoadLinkAppsolution,
]

logger.info('start load to itop')
for loadclass in load_order_list:
    inst = loadclass()
    inst.main()
logger.info('end load to itop')
