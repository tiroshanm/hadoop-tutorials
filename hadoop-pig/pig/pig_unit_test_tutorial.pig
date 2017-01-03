/**
* hadoop command
*  pig -Dmapred.job.queue.name=default -param filePath=/HDFS/path/to/wordcount/inout/file -param hbaseNameSpace=hbase_table_namespace -param hbaseTableName=hbase_table_name -param=/HDFS/path/to/output/file
*/

loaded_lines = LOAD '$filePath';

extracted_words = FOREACH loaded_lines GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
filtered_words = FILTER extracted_words BY NOT word MATCHES '\\w';

words_to_be_filtered = LOAD 'hbase://$hbaseNameSpace:$hbaseTableName' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:word',' -loadKey=true') AS (tblrow_id:chararray,word:chararray);

words_list = JOIN words_to_be_filtered BY word, filtered_words BY word;

grouped_words = GROUP words_list BY words_to_be_filtered::word;
filtered_word_count = FOREACH grouped_words GENERATE COUNT(words_list), group;

STORE filtered_word_count INTO '$fileOutPath';