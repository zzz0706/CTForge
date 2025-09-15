package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class FsServerDefaultsWriteTest {

    @Test
    public void FsServerDefaults_write_serializesConfiguredBytesPerChecksum() throws IOException {
        // 1. Create Configuration and read the value
        Configuration conf = new Configuration();
        int expectedBytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);

        // 2. Prepare test conditions: build FsServerDefaults with the dynamic value
        FsServerDefaults defaults = new FsServerDefaults(
                128 * 1024 * 1024L,          // blockSize
                expectedBytesPerChecksum,    // bytesPerChecksum from config
                64 * 1024,                   // writePacketSize
                (short) 3,                   // replication
                4096,                        // fileBufferSize
                false,                       // encryptDataTransfer
                0,                           // trashInterval
                org.apache.hadoop.util.DataChecksum.Type.CRC32,
                "");                         // keyProviderUri

        // 3. Mock DataOutput and invoke the method under test
        DataOutput mockOut = mock(DataOutput.class);
        defaults.write(mockOut);

        // 4. Verify the second integer written is the configured bytesPerChecksum
        // Order: writeLong(blockSize), writeInt(bytesPerChecksum), ...
        verify(mockOut).writeLong(anyLong());                 // first call
        verify(mockOut).writeInt(expectedBytesPerChecksum);  // second call
    }
}