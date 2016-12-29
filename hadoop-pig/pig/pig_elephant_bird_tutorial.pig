/**
* This pigscript is used to demonstrate how to use ElephantBird Library to load complex json message
* Used V4.15
*/

/**
* hadoop command
*  pig -Dmapred.job.queue.name=default -Dpig.additional.jars=/path/to/elephantbird/libraries/*.jar -param elephantBirdLibPath=/path/to/elephantbird/libraries/*.jar -param jsonFilePath=/HDFS/file/path/to/input
*/

-- Register Required libraries
REGISTER $elephantBirdLibPath;

--Json message format
--{
--  "A": "0011AA",
--  "B": "0011",
--  "C": "0",
--  "D": "TEST_INPUT",
--  "E": "EVENT",
--  "F": "VALUE_FOR_F",
--  "G": {
--    "H": "2016-07-31t18:39:23z",
--    "I": {
--      "J": "VALUE_FOR_J",
--      "K": {
--        "L": "EVENT_A",
--        "I": [
--          {
--            "M": "2016-08-01T00:17:56+05:30",
--            "N": "SOME_TEST_EVENT_A",
--            "O": {
--              "P": "2249948",
--              "Q": "99740892",
--              "R": {
--                "S": "scan",
--                "T": "RFID"
--              }
--            }
--          }
--        ]
--      }
--    }
--  }
--}

/**
* use of Pig Inbuilt function JsonLoader  ------------------------------------------------------------------------------
*/
extracted_message_jsonLoader = LOAD '$jsonFilePath' USING JsonLoader('A:chararray,B:chararray,C:chararray,D:chararray,E:chararray,F:chararray,G:tuple(H:datetime,I:tuple(J:chararray,K:tuple(L:chararray,I:bag{(M:datetime,N:chararray,O:tuple(P:chararray,Q:chararray,R:tuple(S:chararray,T:chararray)))})))');

extracted_attributes_jsonLoader = FOREACH extracted_message_jsonLoader
    GENERATE $0,$1,$2,$3,$4,$5,$6 , FLATTEN(G.I.K.I) AS ( M:datetime, N:chararray, O:tuple ( P:chararray, Q:chararray, R:tuple ( S:chararray, T:chararray ) ) );


json_to_tuple_jsonLoader = FOREACH extracted_attributes_jsonLoader
    GENERATE A, B, C, D, E, F, G.H AS H, G.I.J AS J, G.I.K.L AS L, M, N, O.P AS P, O.Q AS Q, O.R.S AS S, O.R.T AS T;

DUMP json_to_tuple_jsonLoader;
/**
* use of Tweeter Elephant Bird -----------------------------------------------------------------------------------------
*/
extracted_message_elephant_bird = LOAD '$jsonFilePath' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

extracted_attributes_elephant_bird = FOREACH extracted_message_elephant_bird
                GENERATE
                        (chararray) $0#'A' AS A,
                        (chararray) $0#'B' AS B,
                        (chararray) $0#'C' AS C,
                        (chararray) $0#'D' AS D,
                        (chararray) $0#'E' AS E,
                        (chararray) $0#'F' AS F,
                        (datetime) $0#'G'#'H' AS H,
                        (chararray) $0#'G'#'I'#'J' AS J,
                        (chararray) $0#'G'#'I'#'K'#'L' AS L,
                        FLATTEN($0#'G'#'I'#'K'#'I') AS I;

json_to_tuple_elephant_bird = FOREACH extracted_attributes_elephant_bird
               GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8,
                      (datetime) I#'M' AS M,
                      (chararray) I#'N' AS N,
                      (chararray) I#'O'#'P' AS P,
                      (chararray) I#'O'#'Q' AS Q,
                      (chararray) I#'O'#'R'#'S' AS S,
                      (chararray) I#'O'#'R'#'T' AS T;

DUMP json_to_tuple_elephant_bird;
------------------------------------------------------------------------------------------------------------------------