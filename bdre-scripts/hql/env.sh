#!/bin/bash
########################################
# Set environment variables
##########################################
export hueuser=svcvmhdpdev
export kinitvar=svcvmhdpdev@HDP.DEV
export keytab=svcvmhdpdev.keytab
export hiveurl=jdbc:hive2://bigdata-hive2-dev.dish.com:10000/default
export principal=hive/bigdata-hive2-dev.dish.com@HDP.DEV
export yesterday=`date '+%Y-%m-%d' --date="1 days ago"`

echo "yesterday value is" $yesterday