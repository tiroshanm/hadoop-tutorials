#!/usr/bin/env bash

PIG_UNIT_TEST_NAME_SPACE="test"
PIG_UNIT_TEST_TABLE_NAME="tbl_words_to_filter"
PIG_UNIT_TEST_TABLE_WITH_NAMESPACE="${PIG_UNIT_TEST_NAME_SPACE}:${PIG_UNIT_TEST_TABLE_NAME}"
PIG_UNIT_TEST_TABLE_DATA_WORD="data:word"

function insertToStoreDevice() {
    echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE DATA INSERT] INFO "Insert data to  Hbase table :"${PIG_UNIT_TEST_TABLE_WITH_NAMESPACE}
    echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE DATA INSERT] INFO "Executes HBase command : (echo -e ''put '$PIG_UNIT_TEST_TABLE_WITH_NAMESPACE', '$1','$PIG_UNIT_TEST_TABLE_DATA_WORD','$2''' | hbase shell | grep value | grep -o value=.* | cut -f2- -d'=')"

    $(echo -e "put '$PIG_UNIT_TEST_TABLE_WITH_NAMESPACE', '$1','$PIG_UNIT_TEST_TABLE_DATA_WORD','$2'" | hbase shell | grep value | grep -o value=.* | cut -f2- -d'=')

    if [ $? -eq 0 ]
        then
            echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE DATA INSERT] INFO "data successfully upload to Hbase table : ${PIG_UNIT_TEST_TABLE_WITH_NAMESPACE}"
            return $?
        else
            echo `date +'%Y-%m-%d %H:%M:%S'` [PIG_UNIT_TEST] [HBASE DATA INSERT] ERROR "Unable to upload data to Hbase table : ${PIG_UNIT_TEST_TABLE_WITH_NAMESPACE}"
            exit 100
        fi
}
##############HBASE TABLE INSERT STORE DEVICE##########################################
insertToStoreDevice 'key_hello' 'hello'
insertToStoreDevice 'key_world' 'world'