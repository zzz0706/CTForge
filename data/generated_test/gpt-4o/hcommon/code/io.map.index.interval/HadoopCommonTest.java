package org.apache.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;

public class HadoopCommonTest {


    @Test
    public void testFileSystemConfiguration() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

 
        FileSystem fs = FileSystem.get(conf);
        Path testDir = new Path("/tmp/testDir");
        Path testFile = new Path("/tmp/testDir/testFile.txt");
        
        // Cleanup any existing data from previous tests
        if (fs.exists(testDir)) {
            fs.delete(testDir, true);
        }

        // Create directories and files
        fs.mkdirs(testDir);
        fs.create(testFile).close();

        // Fetch file status and assert conditions
        FileStatus fileStatus = fs.getFileStatus(testDir);
        Assert.assertNotNull(fileStatus);
        Assert.assertTrue(fileStatus.isDirectory());

        FileStatus fileStatusFile = fs.getFileStatus(testFile);
        Assert.assertNotNull(fileStatusFile);
        Assert.assertFalse(fileStatusFile.isDirectory());
        Assert.assertEquals(testFile.getName(), fileStatusFile.getPath().getName());

        // Clean up test data
        fs.delete(testDir, true);

        Assert.assertFalse(fs.exists(testDir));
    }
}