#!/bin/sh
. $(dirname $0)/../env.properties
BDRE_HOME=~/bdre
BDRE_APPS_HOME=~/bdre_apps
hdfsPath=/user/$bdreLinuxUserName
nameNode=hdfs://$nameNodeHostName:$nameNodePort
jobTracker=$jobTrackerHostName:$jobTrackerPort
hadoopConfDir=/etc/hive/$hiveConfDir
cd $BDRE_APPS_HOME

echo "Deploying Hive jobs"

mkdir -p $BDRE_APPS_HOME/$busDomainId/$processTypeId/$processId/hql
if [ $? -ne 0 ]
then exit 1
fi

#copy hive-site.xml

cp $hadoopConfDir/hive-site.xml $BDRE_APPS_HOME/$busDomainId/$processTypeId/$processId
if [ $? -ne 0 ]
then exit 1
fi


