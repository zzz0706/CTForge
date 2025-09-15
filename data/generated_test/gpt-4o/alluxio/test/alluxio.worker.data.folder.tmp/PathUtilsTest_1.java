package alluxio.util.io;

import org.junit.Test;
import org.junit.Assert;

public class PathUtilsTest {
    /**
     * Test case: Verify that concatPath correctly combines multiple valid paths into a single cohesive path.
     */
    @Test
    public void test_concatPath_multiplePaths() {
        // Prepare the test conditions: Define multiple valid paths
        String basePath = "/mnt/worker";
        String subDir = ".tmp_blocks";
        String sessionId = "2";
        String filename = "2-100";

        // Test code: Concatenate the paths using PathUtils.concatPath
        String result = PathUtils.concatPath(basePath, subDir, sessionId, filename);

        // Assert that the result matches the expected output
        String expectedPath = "/mnt/worker/.tmp_blocks/2/2-100";
        Assert.assertEquals("The concatenated path did not match the expected result", expectedPath, result);
    }
}