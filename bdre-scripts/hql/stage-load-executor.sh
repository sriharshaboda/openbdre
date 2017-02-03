#!/bin/bash
#echo "hadoop version $HADOOP_VERSION"
#. env.sh
#n=0
#until [ $n -ge 1 ]
#  do
#     echo "doing kinit"
#	 kinit "$kinitvar" -k -t "$keytab"
#	 echo  "kinit is successful, running hive query"
#	 beeline -u "$hiveurl;principal=$principal" -f $1 && break
#     n=$[$n+1]
#     sleep 3
#     echo "reattempt $n"
#done
#echo "exited while loop after $n attempts"
#if [ $n == 1 ]; then
#	echo "[Error] $1 script failed" 1>&2
#	exit 1
#fi
beelinecommand="beeline -u jdbc:hive2://localhost:10000/default -n cloudera -f $1 --hivevar "'"$2"'" --hivevar "'"$3"'" --hivevar "'"$4"'" --hivevar "'"$5"'" --hivevar "'"$6"'" --hivevar "'"$7"'" --hivevar "'"$8"'" --hivevar "'"$9"'" --hivevar ${10} --hivevar ${11} --hivevar "'"${12}"'" --hivevar "'"${13}"'" --hivevar "'"${14}"'" --hivevar "'"${15}"'"   "
eval $beelinecommand