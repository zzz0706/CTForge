package alluxio.util.io;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PathUtilsTest {
    /**
     * Test case: testConcatPath_withValidInput
     * Objective: Verify that PathUtils.concatPath() joins paths correctly without redundancy or truncation based on configuration propagation.
     */
    @Test
    public void testConcatPath_withValidInput() {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        AlluxioProperties props = ConfigurationUtils.defaults();
        props.set(PropertyKey.WORKER_DATA_TMP_FOLDER, "tmp");

        String tmpFolder = props.get(PropertyKey.WORKER_DATA_TMP_FOLDER);
        
        // 2. Prepare the test conditions: use a valid base path and subpaths.
        String basePath = "/mnt/storage";
        String sessionId = "1";
        String blockId = "12345";

        // 3. Test code: call PathUtils.concatPath() with the prepared inputs.
        String actualPath = PathUtils.concatPath(basePath, tmpFolder, sessionId, blockId);

        // 4. Verify the expected concatenated path.
        String expectedPath = "/mnt/storage/tmp/1/12345";
        assertEquals("The concatenated path should match the expected output", expectedPath, actualPath);
    }
}