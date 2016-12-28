package hadoop.pig;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PigElephantBirdTutorialTest {
    private static PigTest pigElephantBirdTest;
    private static String TEST_PATH = "./src/test/";
    private static String PIGSCRIPT_PATH = "./pig";

    @BeforeClass
    public static void setUp()throws IOException {

        String workingDir = System.getProperty("user.dir");

        String[] args = {
                "elephantBirdLibPath=" + workingDir + "/utility/lib/*.jar",
                "jsonFilePath=" + TEST_PATH + "/resources/input/testJson"
        };
        pigElephantBirdTest = new PigTest(PIGSCRIPT_PATH + "/pig_elephant_bird_tutorial.pig", args);
    }

    @Test
    public void Should_ReturnExtractedJsonMessageAsTuple_WhenJsonFileIsProvided() throws IOException, ParseException {
        pigElephantBirdTest.assertOutput("json_to_tuple", new File(TEST_PATH + "resources/output/jsonToTuple.txt"));
    }
}
