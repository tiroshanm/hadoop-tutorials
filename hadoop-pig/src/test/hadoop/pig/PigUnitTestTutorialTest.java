package hadoop.pig;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PigUnitTestTutorialTest {
    private static PigTest pigUnitTest;
    private static String TEST_PATH = "./src/test/";
    private static String PIGSCRIPT_PATH = "./pig";

    @BeforeClass
    public static void setUp()throws IOException {

        // set working directory to current directory ------------------------------------------------------------------
        String workingDir = System.getProperty("user.dir");

        // declare external arguments pass to pigscript at execution time ----------------------------------------------
        String[] args = {
                "filePath=" + TEST_PATH + "/resources/input/wordList",
                "hbaseNameSpace=test",
                "hbaseTableName=tbl_words_to_filter",
                "fileOutPath=" + TEST_PATH + "/resources/output/wordCount"
        };

        // initialize pig unit test with external arguments ------------------------------------------------------------
        pigUnitTest = new PigTest(PIGSCRIPT_PATH + "/pig_unit_test_tutorial.pig", args);

        // mock hbase data using a csv file (or tsv) - override Hbase load by a Pig FileLoader
        pigUnitTest.override("words_to_be_filtered","words_to_be_filtered=LOAD '" + TEST_PATH + "resources//hbase/wordsToFilter.csv' USING PigStorage(',') AS (tblrow_id:chararray,word:chararray);");
    }

    // assert intermediate results using a external file ---------------------------------------------------------------
    @Test
    public void Should_ReturnExtractedParagraphsAsTuple_WhenWordInputFileIsGiven() throws IOException, ParseException {
        pigUnitTest.assertOutput("loaded_lines", new File(TEST_PATH + "resources/assert/loadedLines.txt"));
    }

    @Test
    public void Should_ReturnExtractedWords_WhenExtractedParagraphsAreGiven() throws IOException, ParseException {
        pigUnitTest.assertOutput("extracted_words", new File(TEST_PATH + "resources/assert/extractedWords.txt"));
    }

    @Test
    public void Should_ReturnFilteredWords_WhenExtractedWordsAreGiven() throws IOException, ParseException {
        pigUnitTest.assertOutput("filtered_words", new File(TEST_PATH + "resources/assert/filteredWords.txt"));
    }
    // -----------------------------------------------------------------------------------------------------------------

    // Enable STORE - default Pig Unit disable STORE and LOAD ----------------------------------------------------------
    @Test
    public void Should_StoreCountedWordResultsToHDFS_WhenPathIsPassed() throws IOException, ParseException {
        deleteFolder(new File(TEST_PATH + "/resources/output/wordCount"));

        String[] args = {
                "filePath=" + TEST_PATH + "/resources/input/wordList",
                "hbaseNameSpace=test",
                "hbaseTableName=tbl_words_to_filter",
                "fileOutPath=" + TEST_PATH + "/resources/output/wordCount"
        };

        PigTest storeResultTest = new PigTest(PIGSCRIPT_PATH + "/pig_unit_test_tutorial.pig", args);
        storeResultTest.override("words_to_be_filtered","words_to_be_filtered=LOAD '" + TEST_PATH + "resources//hbase/wordsToFilter.csv' USING PigStorage(',') AS (tblrow_id:chararray,word:chararray);");
        storeResultTest.unoverride("STORE");
        storeResultTest.runScript();
    }

    private static void deleteFolder(File element) {
        if (element.isDirectory()) {
            for (File sub : element.listFiles()) {
                deleteFolder(sub);
            }
        }
        element.delete();
    }
    // -----------------------------------------------------------------------------------------------------------------
}
