package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.junit.Test;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class TestViewFileSystemRename {

    @Test
    public void testRenameAcrossDifferentMountpointsWithSameTargetUri() throws Exception {
        // Step 1: Create a Configuration object and define the mount points
        Configuration conf = new Configuration();

        // Correctly configure the mount table with mount points that ensure proper initialization
        conf.set("fs.viewfs.mounttable.har.multiplex.link./mountpointA", "file:///tmp/dirA");
        conf.set("fs.viewfs.mounttable.har.multiplex.link./mountpointB", "file:///tmp/dirA");

        // Set the rename strategy configuration for ViewFileSystem
        conf.set("fs.viewfs.rename.strategy",
                ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT.toString());

        // Retrieve the RenameStrategy from the configuration
        String renameStrategy = conf.get("fs.viewfs.rename.strategy");

        // Ensure the RenameStrategy retrieved matches the expected configuration
        Assert.assertNotNull("Rename Strategy should not be null", renameStrategy);
        Assert.assertEquals("Rename Strategy is unexpectedly different",
                ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT.toString(),
                renameStrategy);

        // Step 2: Initialize ViewFileSystem with a sample URI and the Configuration
        FileSystem fs = FileSystem.get(new URI("viewfs://har.multiplex/"), conf);
        Assert.assertTrue("FileSystem is not an instance of ViewFileSystem", fs instanceof ViewFileSystem);
        ViewFileSystem viewFileSystem = (ViewFileSystem) fs;

        // Prepare test conditions by creating necessary files in /tmp/dirA
        prepareTestFiles();

        // Step 3: Simulate renaming across mount points with the same target URI
        Path srcPath = new Path("/mountpointA/file1");
        Path dstPath = new Path("/mountpointB/file1");

        try {
            boolean result = viewFileSystem.rename(srcPath, dstPath);

            // Step 4: Assert the rename operation succeeds
            Assert.assertTrue("Rename operation should succeed under SAME_TARGET_URI_ACROSS_MOUNTPOINT strategy", result);
        } catch (Exception ex) {
            Assert.fail("Rename operation failed unexpectedly: " + ex.getMessage());
        } finally {
            // Clean up test files
            cleanupTestFiles();
        }
    }

    private void prepareTestFiles() throws IOException {
        File dirA = new File("/tmp/dirA");
        if (!dirA.exists()) {
            boolean dirCreated = dirA.mkdirs();
            if (!dirCreated) {
                throw new IOException("Failed to create directory /tmp/dirA");
            }
        }
        File file1 = new File("/tmp/dirA/file1");
        if (!file1.exists()) {
            boolean fileCreated = file1.createNewFile();
            if (!fileCreated) {
                throw new IOException("Failed to create test file /tmp/dirA/file1");
            }
        }
    }

    private void cleanupTestFiles() throws IOException {
        File file1 = new File("/tmp/dirA/file1");
        if (file1.exists() && !file1.delete()) {
            throw new IOException("Failed to delete test file /tmp/dirA/file1");
        }
        File dirA = new File("/tmp/dirA");
        if (dirA.exists() && !dirA.delete()) {
            throw new IOException("Failed to delete directory /tmp/dirA");
        }
    }
}