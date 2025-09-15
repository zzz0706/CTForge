package alluxio.worker.block.meta;

import alluxio.util.io.PathUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class AbstractBlockMetaTest {
    // Test the concatPath function to verify proper concatenation utilizing paths and verifying behavior when configuration-related inputs are provided.

    @Test
    public void testPathUtilsConcatPathWithMultipleValidInputs() {
        // Test code
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.

        // Obtain configuration value for "alluxio.worker.data.folder.tmp" using ServerConfiguration API.
        String tmpFolder = ServerConfiguration.get(PropertyKey.WORKER_DATA_TMP_FOLDER);

        // Prepare other test inputs (base path and subpaths).
        String basePath = "/mnt/data";
        String subPath1 = "subdir1";
        String subPath2 = "subdir2";

        // 2. Prepare the test conditions.
        // The objective is to concatenate these paths together correctly.

        // 3. Test code.
        // Call the concatPath function with the inputs derived from configuration and valid additional paths.
        String resultPath = PathUtils.concatPath(basePath, tmpFolder, subPath1, subPath2);

        // 4. Code after testing.
        // Validate the concatenated result.
        String expectedPath = "/mnt/data/" + tmpFolder + "/subdir1/subdir2";
        Assert.assertEquals("The path should be concatenated correctly", expectedPath, resultPath);
    }
}