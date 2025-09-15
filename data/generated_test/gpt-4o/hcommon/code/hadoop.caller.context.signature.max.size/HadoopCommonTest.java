package org.apache.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class HadoopCommonTest {       
    // test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    @Test
    public void testFileSystemCreateAndRead() throws Exception {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS");

        // Ensure default filesystem implementation is properly set
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // 2. 准备测试条件
        FileSystem fs = FileSystem.get(conf);
        Path testFilePath = new Path("/tmp/testFile.txt");
        if (fs.exists(testFilePath)) {
            fs.delete(testFilePath, false);
        }

        // 3. 测试代码
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

        // 4. 测试后的代码
        Assert.assertEquals("Hello, Hadoop!", fileContent);

        // Clean up
        if (fs.exists(testFilePath)) {
            fs.delete(testFilePath, false);
        }
    }
}