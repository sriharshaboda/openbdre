set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000;
INSERT INTO TABLE ${baseDb}.${baseTable} PARTITION ( instanceexecid) SELECT * FROM ${baseDb}.${baseTable}_${instanceExecId};
DROP TABLE ${baseDb}.${baseTable}_${instanceExecId};