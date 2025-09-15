package org.apache.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class HadoopCommonTest {       

    @Test
    public void testFileSystemCreateAndRead() throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS");

        // Ensure default filesystem implementation is properly set
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());


        FileSystem fs = FileSystem.get(conf);
        Path testFilePath = new Path("/tmp/testFile.txt");
        if (fs.exists(testFilePath)) {
            fs.delete(testFilePath, false);
        }

 
        // Write data to a file
        try (java.io.OutputStream out = fs.create(testFilePath)) {
            out.write("Hello, Hadoop!".getBytes());
        }

        // Read data from the file
        String fileContent;
        try (java.io.InputStream in = fs.open(testFilePath)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            fileContent = new String(buffer, 0, bytesRead);
        }

        Assert.assertEquals("Hello, Hadoop!", fileContent);

        // Clean up
        if (fs.exists(testFilePath)) {
            fs.delete(testFilePath, false);
        }
    }
}