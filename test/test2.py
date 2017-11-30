import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:Password1@127.0.0.1:3306/itop?charset=utf8",encoding="utf-8", echo=True)
ddf = pd.DataFrame({"name":['apple','orange','pie']})
ddf.to_sql('tmp', con=engine, if_exists='append',index=False)

# base
location (server,network,stroage) -> synchro_data_location_77   
brand (server,network,stroage) -> synchro_data_brand_73     
model (server,network,stroage) -> synchro_data_model_74
osfamily (server,vm) -> synchro_data_osfamily_75
osversion(server,vm) -> synchro_data_osversion_76    

# sync server
physicalserver(synchro_data_physicalserver_78)
hypervisor

# sync network
networkdevicetype
networkdevice

# sync storage
storagesystem

# sync vm
virtualmachine

# sync lv
logicalvolume
link_server_logicalvolume
link_virtualdevice_volume

# sync db
dbserver (synchro_data_dbserver_85)
oracleinstance
mongodbinstance

# sync middelware
middleware
weblogicinstance
oc4jinstance
solrinstance
bwinstance
emsinstance
spotfireinstance
gfsinstance
zookeeperinstance

# sync web app
webserver
nginxinstance
ohsinstance
spotfireweb

# sync app solution
appsolution
link_appsolution_ci



# location



 Field                    | Type                        | Null | Key | Default | Extra |
+--------------------------+-----------------------------+------+-----+---------+-------+
| id                       | int(11)                     | NO   | MUL | 0       |       |
| primary_key              | varchar(255)                | YES  | MUL | NULL    |       |
| name                     | varchar(255)                | YES  |     | NULL    |       |
| description              | text                        | YES  |     | NULL    |       |
| org_id                   | varchar(255)                | YES  |     | NULL    |       |
| business_criticity       | enum('high','low','medium') | YES  |     | NULL    |       |
| move2production          | date                        | YES  |     | NULL    |       |
| contacts_list            | text                        | YES  |     | NULL    |       |
| documents_list           | text                        | YES  |     | NULL    |       |
| applicationsolution_list | text                        | YES  |     | NULL    |       |
| providercontracts_list   | text                        | YES  |     | NULL    |       |
| services_list            | text                        | YES  |     | NULL    |       |
| tickets_list             | text                        | YES  |     | NULL    |       |
| system_id                | varchar(255)                | YES  |     | NULL    |       |
| software_id              | varchar(255)                | YES  |     | NULL    |       |
| softwarelicence_id       | varchar(255)                | YES  |     | NULL    |       |
| path                     | varchar(255)                | YES  |     | NULL    |       |
| status                   | enum('active','inactive')   | YES  |     | NULL    |      


primary_key
name
description
org_id
system_id
path


Hypervisor)


 <class id="DBServer">
      <fields>
        <field id="environment" xsi:type="AttributeString" _delta="define">
          <sql>environment</sql>
          <default_value/>
          <is_null_allowed>true</is_null_allowed>
        </field>
      </fields>
     </class>


datamodels/2.x/itop-config-mgmt/datamodel.itop-config-mgmt.xml


<class id="DBServer"
'Class:DBServer/Attribute:environment' => 'Environment',

      <presentation>
        <details>
          <items>
            <item id="name">
              <rank>10</rank>
            </item>
            <item id="org_id">
              <rank>20</rank>
            </item>
            <item id="environment">
              <rank>21</rank>
            </item>
            <!-- <item id="status">
               <rank>30</rank>
             </item>
             <item id="business_criticity">
               <rank>40</rank>
             </item> -->
            <item id="system_id">
              <rank>50</rank>
            </item>
            <!-- <item id="software_id">
              <rank>60</rank>
            </item>
            <item id="softwarelicence_id">
              <rank>70</rank>
            </item> -->
            <item id="path">
              <rank>80</rank>
            </item>
             <!-- <item id="move2production">
               <rank>90</rank>
             </item> -->
            <item id="description">
              <rank>100</rank>
            </item>
             <!-- <item id="contacts_list">
               <rank>110</rank>
             </item>
             <item id="documents_list">
               <rank>120</rank>
             </item> -->
            <item id="applicationsolution_list">
              <rank>140</rank>
            </item>
            <item id="dbschema_list">
              <rank>150</rank>
            </item>
             <!-- <item id="providercontracts_list">
               <rank>160</rank>
             </item> -->
            <item id="services_list">
              <rank>170</rank>
            </item>
          </items>
        </details>
        <search>
          <items>
            <item id="name">
              <rank>10</rank>
            </item>
            <item id="org_id">
              <rank>20</rank>
            </item>
            <item id="environment">
              <rank>21</rank>
            </item>
             <!--<item id="business_criticity">
               <rank>30</rank>
             </item>
             <item id="move2production">
               <rank>40</rank>
             </item> -->
          </items>
        </search>
        <list>
          <items>
            <item id="org_id">
              <rank>10</rank>
            </item>
             <!-- <item id="business_criticity">
               <rank>20</rank>
             </item> -->
            <item id="system_id">
              <rank>30</rank>
            </item>
             <!-- <item id="software_id">
               <rank>40</rank>
             </item> -->
          </items>
        </list>
      </presentation>



Field                    | Type                        | Null | Key | Default | Extra |
+--------------------------+-----------------------------+------+-----+---------+-------+
| id                       | int(11)                     | NO   | MUL | 0       |       |
| primary_key              | varchar(255)                | YES  | MUL | NULL    |       |
| name                     | varchar(255)                | YES  |     | NULL    |       |
| description              | text                        | YES  |     | NULL    |       |
| org_id                   | varchar(255)                | YES  |     | NULL    |       |
| business_criticity       | enum('high','low','medium') | YES  |     | NULL    |       |
| move2production          | date                        | YES  |     | NULL    |       |
| contacts_list            | text                        | YES  |     | NULL    |       |
| documents_list           | text                        | YES  |     | NULL    |       |
| applicationsolution_list | text                        | YES  |     | NULL    |       |
| providercontracts_list   | text                        | YES  |     | NULL    |       |
| services_list            | text                        | YES  |     | NULL    |       |
| tickets_list             | text                        | YES  |     | NULL    |       |
| system_id                | varchar(255)                | YES  |     | NULL    |       |
| software_id              | varchar(255)                | YES  |     | NULL    |       |
| softwarelicence_id       | varchar(255)                | YES  |     | NULL    |       |
| path                     | varchar(255)                | YES  |     | NULL    |       |
| status                   | enum('active','inactive')   | YES  |     | NULL    |       |
| environment              | varchar(255)  


primary_key
name
description
org_id
system_id
path
environment


cmdbrep:PRIMARY> db.merge_oracle_db.findOne()
{
        "_id" : ObjectId("599aab466317c17632218836"),
        "merge_environment" : "PROD",
        "merge_software" : "Oracle Database 11g",
        "merge_home_location" : "/home/oracle/db11g/11.2.0",
        "merge_host_name" : "aisrdb",
        "merge_home_name" : "oradb11g",
        "merge_name" : "aisr_db",
        "merge_version" : "11.2.0.2.0"
}


cmdbrep:PRIMARY> db.merge_mongo_db.findOne()
{
        "_id" : ObjectId("599aab466317c17632218374"),
        "merge_journalingEnabled" : false,
        "merge_logsEnabled" : true,
        "merge_replicaStateName" : "PRIMARY",
        "merge_hostname" : "asapmongo01",
        "merge_sslEnabled" : false,
        "merge_authMechanismName" : "MONGODB_CR",
        "merge_version" : "3.4.0",
        "merge_hidden" : false,
        "merge_profilerEnabled" : true,
        "merge_replicaSetName" : "asaprep",
        "merge_port" : 27017,
        "merge_typeName" : "REPLICA_PRIMARY",
        "merge_name" : "asapmongo01:27017",
        "merge_created" : "2016-10-27T07:10:23Z"
}
