#!/bin/sh
. $(dirname $0)/../env.properties
BDRE_HOME=~/bdre
BDRE_APPS_HOME=~/bdre_apps
hdfsPath=/user/$bdreLinuxUserName
nameNode=hdfs://$nameNodeHostName:$nameNodePort
jobTracker=$jobTrackerHostName:$jobTrackerPort
hadoopConfDir=/etc/hive/$hiveConfDir
cd $BDRE_APPS_HOME

echo "Deploying Shell jobs"

dos2unix $BDRE_APPS_HOME/$busDomainId/$processTypeId/$processId/shell/*
dos2unix $BDRE_APPS_HOME/$busDomainId/$processTypeId/$processId/additional/*