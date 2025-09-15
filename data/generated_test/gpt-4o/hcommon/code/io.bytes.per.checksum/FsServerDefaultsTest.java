package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;

import static org.mockito.Mockito.*;

public class FsServerDefaultsTest {

    // test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码
    @Test
    public void test_write_correctlyEncodesServerDefaults() throws IOException {
        // Step 1: Initialize the configuration
        Configuration conf = new Configuration();

        // Step 2: Set up mock FileSystem and FsServerDefaults behavior
        FileSystem fs = mock(FileSystem.class);
        FsServerDefaults mockServerDefaults = new FsServerDefaults(
                65536L, // blockSize (needs to be long)
                512, // bytesPerChecksum
                1, // writePacketSize
                (short) 3, // replication
                1048576, // fileBufferSize
                true, // encryptDataTransfer
                1048576L, // defaultFileBufferSize (needs to be long)
                DataChecksum.Type.CRC32C // checksumType
        );

        when(fs.getConf()).thenReturn(conf);
        when(fs.getServerDefaults()).thenReturn(mockServerDefaults);

        // Step 3: Retrieve the FsServerDefaults instance
        FsServerDefaults serverDefaults = fs.getServerDefaults();

        // Step 4: Mock a DataOutput instance
        DataOutput mockDataOutput = mock(DataOutput.class);

        // Step 5: Invoke the write(DataOutput out) method
        serverDefaults.write(mockDataOutput);

        // Step 6: Verify that the correct value of bytesPerChecksum is written
        verify(mockDataOutput).writeInt(serverDefaults.getBytesPerChecksum());
    }
}