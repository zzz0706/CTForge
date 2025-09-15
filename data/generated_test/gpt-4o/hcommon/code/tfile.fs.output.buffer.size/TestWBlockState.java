package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class TestWBlockState {
    @Test
    public void testWBlockStateWithCustomBufferSize() throws IOException {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration conf = new Configuration();
        int bufferSize = TFile.getFSOutputBufferSize(conf);

        // 2. 准备测试条件
        ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
        FSDataOutputStream fsOut = new FSDataOutputStream(byteArrayOutStream, null);

        // 创建所需的压缩算法信息
        String compressionName = "gz"; // 使用String代替CompressionCodec对象
        String comparatorName = "memcmp"; // 假设默认使用memcmp comparator

        // 3. 测试代码
        Writer wBlockState = new Writer(fsOut, bufferSize, compressionName, comparatorName, conf);

        // 4. 测试后的代码
        assertNotNull("Buffer State should not be null", wBlockState);
    }
}