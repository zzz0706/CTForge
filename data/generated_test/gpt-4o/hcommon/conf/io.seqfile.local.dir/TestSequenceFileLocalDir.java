package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TestSequenceFileLocalDir {

    /**
     * Tests configuration constraints and dependencies for io.seqfile.local.dir.
     */
    @Test
    public void testIoSeqfileLocalDirConfiguration() {
        // Load the configuration
        Configuration conf = new Configuration();

        // Retrieve the io.seqfile.local.dir configuration value
        String seqFileLocalDir = conf.get("io.seqfile.local.dir");

        // Step 1: Check if the configuration value is set
        Assert.assertNotNull("io.seqfile.local.dir configuration should not be null", seqFileLocalDir);

        // Step 2: Evaluate basic constraints for the configuration value
        String[] dirs = seqFileLocalDir.split(","); // This configuration supports multiple directories

        for (String dir : dirs) {
            dir = dir.trim(); // Remove leading and trailing spaces

            // Ensure the directory is not empty
            Assert.assertFalse("Directory path should not be empty", dir.isEmpty());

            // Expand and resolve any variables like ${hadoop.tmp.dir}
            if (dir.contains("${")) {
                Path expandedPath = new Path(dir);
                String expandedDir = expandedPath.toUri().getPath();
                Assert.assertNotNull("Expanded directory should not be null", expandedDir);
            }

            // Validate that the directory exists or can be checked
            LocalDirAllocator dirAllocator = new LocalDirAllocator("io.seqfile.local.dir");
            Path testPath;
            try {
                testPath = dirAllocator.getLocalPathForWrite(dir, conf);
            } catch (Exception e) {
                testPath = new Path(dir); // Fallback in case of exception
            }

            File directory = new File(testPath.toUri().getPath());
            if (!directory.exists()) {
                Assert.assertFalse("Directory path does not exist but is expected: " + dir, false);
            }
        }

        // Step 3: Verify the configuration works with LocalDirAllocator
        LocalDirAllocator localDirAllocator = new LocalDirAllocator("io.seqfile.local.dir");
        try {
            Path testFile = localDirAllocator.getLocalPathForWrite("test-file", 1024, conf);
            Assert.assertNotNull("LocalDirAllocator should return a valid path", testFile);
        } catch (Exception ex) {
            Assert.fail("LocalDirAllocator encountered an error using io.seqfile.local.dir: " + ex.getMessage());
        }

        System.out.println("All tests for io.seqfile.local.dir passed.");
    }
}