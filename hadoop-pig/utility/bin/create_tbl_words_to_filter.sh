#!/usr/bin/env bash

PIG_UNIT_TEST_NAME_SPACE="test"
PIG_UNIT_TEST_TABLE_NAME="tbl_words_to_filter"

#############CREATE NAMESPACE#################################################
echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE SCHEMA] INFO "Create namespace : "${PIG_UNIT_TEST_NAME_SPACE}
echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE SCHEMA] INFO "Hbase command: (echo ''create_namespace '${PIG_UNIT_TEST_NAME_SPACE}''' | hbase shell)"
(echo "create_namespace '${PIG_UNIT_TEST_NAME_SPACE}'" | hbase shell) > log

#############CREATE HBASE_IOT_EVENT_HOURLY_AGGREGATED_DATA TABLE##########################
echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE SCHEMA] INFO "Create Table : "${PIG_UNIT_TEST_TABLE_NAME}
echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE SCHEMA] INFO "Hbase command: (echo ''exists '${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME}''' | hbase shell)"
(echo "exists '${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME}'" | hbase shell) > log
cat log | grep "Table ${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME} does exist"
if [ $? -eq 0 ]
then
    echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE SCHEMA] INFO "Table does exist : "${PIG_UNIT_TEST_TABLE_NAME}
else
    echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE SCHEMA] INFO "Table does not exists : "${PIG_UNIT_TEST_TABLE_NAME}
    echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE SCHEMA] INFO "Hbase command: (echo ''create '${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME}',{NAME=>'data', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'NONE', REPLICATION_SCOPE => '0', VERSIONS => '3', COMPRESSION => 'NONE', TTL =>'2147483647', BLOCKSIZE => '65536', IN_MEMORY =>'true', BLOCKCACHE => 'true',MIN_VERSIONS => '0',KEEP_DELETED_CELLS => 'false'}'' | hbase shell)"
    (echo "create '${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME}',{NAME=>'data', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'NONE', REPLICATION_SCOPE => '0', VERSIONS => '3', COMPRESSION => 'NONE', TTL =>'2147483647', BLOCKSIZE => '65536', IN_MEMORY =>'true', BLOCKCACHE => 'true',MIN_VERSIONS => '0',KEEP_DELETED_CELLS => 'false'}" | hbase shell)
fi
(echo "describe '${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME}'" | hbase shell)
(echo "enable '${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME}'" | hbase shell)

