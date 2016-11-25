#!/usr/bin/env bash

#############CONFIGURATIONS###################################################
HBASE_NAME_SPACE="testDataBase"
HBASE_TEST_TABLE_NAME="testTable"
HBASE_TABLE_COLUMN_FAMILY="data"
HBASE_TABLE_WITH_NAME_SPACE="${HBASE_NAME_SPACE}:${HBASE_TEST_TABLE_NAME}"
HBASE_COLUMN_QULIFIER_CODE="${HBASE_TABLE_COLUMN_FAMILY}:code"
HBASE_COLUMN_QULIFIER_NAME="${HBASE_TABLE_COLUMN_FAMILY}:name"

function insertToStoreDevice() {
    echo `date +'%Y-%m-%d %H:%M:%S'` [HBASE TEST] [HBASE SCHEMA] [HBASE DATA MANIPULATION]"Insert meta date to  Hbase table :"${HBASE_TABLE_WITH_NAME_SPACE}
    echo `date +'%Y-%m-%d %H:%M:%S'` [HBASE TEST] [HBASE SCHEMA] [HBASE DATA MANIPULATION]"Executes HBase command : (echo -e ''put '$HBASE_TABLE_WITH_NAME_SPACE', '$1','$HBASE_COLUMN_QULIFIER_CODE','$1'\nput '$HBASE_TABLE_WITH_NAME_SPACE', '$1','$HBASE_COLUMN_QULIFIER_NAME','$2''' | hbase shell | grep value | grep -o value=.* | cut -f2- -d'=')"

    $(echo -e "put '$HBASE_TABLE_WITH_NAME_SPACE', '$1','$HBASE_COLUMN_QULIFIER_CODE','$1'\nput '$HBASE_TABLE_WITH_NAME_SPACE', '$1','$HBASE_COLUMN_QULIFIER_NAME','$2'" | hbase shell | grep value | grep -o value=.* | cut -f2- -d'=')

    if [ $? -eq 0 ]
        then
            echo `date +'%Y-%m-%d %H:%M:%S'` [HBASE TEST] [HBASE SCHEMA] [HBASE DATA MANIPULATION]"Meta data successfully upload to Hbase table : ${HBASE_TABLE_WITH_NAME_SPACE}"
            return $?
        else
            echo `date +'%Y-%m-%d %H:%M:%S'` [ECS_ANALYTICS] [HBASE DATA INSERT] [ALGORITHM_NAME] ERROR "Unable to upload meta data to Hbase table : ${HBASE_TABLE_WITH_NAME_SPACE}"
            exit 100
        fi
}

##############HBASE META TABLE INSERT ALGORITHM NAME##########################################
insertToStoreDevice '100' 'aaaa'
insertToStoreDevice '101' 'bbbb'
insertToStoreDevice '102' 'cccc'
insertToStoreDevice '103' 'dddd'
insertToStoreDevice '104' 'eeee'
insertToStoreDevice '105' 'ffff'
