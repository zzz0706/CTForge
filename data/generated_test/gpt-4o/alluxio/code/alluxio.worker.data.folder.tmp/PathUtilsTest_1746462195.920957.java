package alluxio.util.io;

import alluxio.util.io.PathUtils;
import org.junit.Assert;
import org.junit.Test;

public class PathUtilsTest {
    /**
     * Test case: test_concatPath_nullPathHandling
     * Objective: Ensure concatPath can handle null values in the paths array gracefully.
     * Prerequisites: Use a valid base path and set one or more elements in the paths array to null.
     * Steps:
     *      1. Call PathUtils.concatPath with a base path and paths array containing null values.
     *      2. Retrieve the concatenated path.
     * Expected result: The result should exclude null values from the final path while concatenating
     *                  the non-null elements successfully.
     */
    @Test
    public void testConcatPathNullPathHandling() {
        // Prepare test conditions
        String basePath = "/mnt/mem";
        Object[] paths = { "data", null, "", "blocks" };

        // Test code: call the function being tested
        String result = PathUtils.concatPath(basePath, paths);

        // Verify the expected result meets functionality requirements
        String expectedResult = "/mnt/mem/data/blocks";
        Assert.assertEquals("concatPath should exclude null and empty values", expectedResult, result);

        // Code after testing (clean up is not needed in this test case as no resources are allocated)
    }
}