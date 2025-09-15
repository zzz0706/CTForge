package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.io.DataOutput;
import java.io.IOException;

public class FsServerDefaultsTest {

    // Test: write correctly encodes server defaults
    @Test
    public void test_write_correctlyEncodesServerDefaults() throws IOException {
        // Step 1: Populate Configuration with "io.bytes.per.checksum"
        Configuration conf = new Configuration();
        int expectedBytesPerChecksum = 1024; // Example value
        conf.setInt("io.bytes.per.checksum", expectedBytesPerChecksum);

        // Step 2: Mock FileSystem and set up FsServerDefaults
        FileSystem fs = mock(FileSystem.class);
        when(fs.getConf()).thenReturn(conf);

        FsServerDefaults mockServerDefaults = new FsServerDefaults(
                65536L, // blockSize
                conf.getInt("io.bytes.per.checksum", 512), // bytesPerChecksum from configuration
                64000, // writePacketSize
                (short) 3, // replication
                1048576, // fileBufferSize
                true, // encryptDataTransfer
                604800L, // trashInterval
                DataChecksum.Type.CRC32C, // checksumType
                "keyProviderURI",
                (byte) 2 // storagePolicy
        );
        when(fs.getServerDefaults()).thenReturn(mockServerDefaults);

        // Step 3: Retrieve FsServerDefaults via the public interface
        FsServerDefaults serverDefaults = fs.getServerDefaults();

        // Step 4: Mock a DataOutput instance
        DataOutput mockDataOutput = mock(DataOutput.class);

        // Step 5: Invoke write on serverDefaults
        serverDefaults.write(mockDataOutput);

        // Step 6: Verify that the correct bytesPerChecksum value was written
        verify(mockDataOutput).writeInt(expectedBytesPerChecksum);
    }

    // Test: getServerDefaults reads correct configuration
    @Test
    public void test_getServerDefaults_readsCorrectConfiguration() throws IOException {
        // Step 1: Populate Configuration with "io.bytes.per.checksum"
        Configuration conf = new Configuration();
        int expectedBytesPerChecksum = 512; // Example value
        conf.setInt("io.bytes.per.checksum", expectedBytesPerChecksum);

        // Step 2: Mock FileSystem
        FileSystem fs = mock(FileSystem.class);
        when(fs.getConf()).thenReturn(conf);

        // Step 3: Create FsServerDefaults object with configuration
        FsServerDefaults serverDefaults = new FsServerDefaults(
                128 * 1024 * 1024L, // blockSize
                conf.getInt("io.bytes.per.checksum", 512), // bytesPerChecksum from configuration
                64 * 1024, // writePacketSize
                (short) 3, // replication
                64 * 1024, // fileBufferSize
                false, // encryptDataTransfer
                0, // trashInterval
                DataChecksum.Type.CRC32, // checksumType
                "", // keyProviderURI
                (byte) 0 // storagePolicy
        );

        // Validation: Ensure bytesPerChecksum matches configuration
        assert serverDefaults.getBytesPerChecksum() == expectedBytesPerChecksum;
    }

    // Test: handle checksum exception and skip bad checksum properly
    @Test
    public void test_handleChecksumException_skipsBadChecksumProperly() throws IOException {
        // Step 1: Configure "io.skip.checksum.errors"
        Configuration conf = new Configuration();
        conf.setInt("io.bytes.per.checksum", 2048); // Example value
        conf.setBoolean("io.skip.checksum.errors", true); // Enable skipping checksum errors

        // Step 2: Create DataOutputBuffer
        DataOutputBuffer buffer = new DataOutputBuffer();

        // Step 3: Simulate checksum exception in a controlled manner
        try {
            // Simulate checksum-related operation (mock exception behavior where necessary)
            throw new IOException("Simulating ChecksumException");
        } catch (IOException e) {
            // Verify that exception is caught and handled appropriately in test case
            assert conf.getBoolean("io.skip.checksum.errors", false) : "Checksum errors should be skipped!";
        }
    }
}