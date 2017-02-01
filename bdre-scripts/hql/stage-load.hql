CREATE VIEW IF NOT EXISTS  ${rawViewDbName}.${rawViewName} AS SELECT ${viewColumnsWithDataTypes} FROM  ${rawTableDbName}.${rawTableName};
CREATE TABLE IF NOT EXISTS ${baseTableDbName}.${baseTableName} ( ${baseColumnsWithDataTypes} ) partitioned by ( ${partitionColumns} instanceexecid bigint) stored as orc;
CREATE TABLE IF NOT EXISTS ${baseTableDbName}.${baseTableName}_${instanceExecId} ( ${baseColumnsWithDataTypes} ) partitioned by ( ${partitionColumns} instanceexecid bigint) stored as orc;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000;
INSERT OVERWRITE TABLE ${baseTableDbName}.${baseTableName}_${instanceExecId} PARTITION ( ${partitionKeys} instanceexecid) SELECT ${fieldNames}, ${partitionKeys} ${instanceExecId} FROM ${rawTableDbName}.${rawViewName} where batchid>= ${minBatchId} and batchid<=${maxBatchId};