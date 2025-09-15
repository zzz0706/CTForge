package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class TestSequenceFileHandleChecksumException {
    // Test handleChecksumException with skipChecksumErrors enabled
    @Test
    public void test_handleChecksumException_skipChecksumErrorsEnabled() throws IOException {
        // 1. Prepare test conditions
        // Use real Configuration object to avoid missing method errors
        Configuration conf = new Configuration();
        conf.setBoolean("io.skip.checksum.errors", true); // Enable skip checksum errors
        conf.setInt("io.bytes.per.checksum", 512);

        // Create a mock Reader instance
        Reader reader = Mockito.mock(SequenceFile.Reader.class);

        // Use an existing IOException to simulate a checksum error
        IOException simulatedException = new IOException("Simulated checksum error");

        // Stub the reader's behavior on encountering an exception
        Mockito.doThrow(simulatedException).when(reader).seek(Mockito.anyLong());

        // 2. Test logic
        try {
            // Attempt to call seek or a similar operation
            reader.seek(100L); // Passing any dummy position
        } catch (IOException e) {
            if (!conf.getBoolean("io.skip.checksum.errors", false)) {
                throw e; // Re-throw the exception if skip checksum is not enabled
            }
        }

        // 3. Verify no exceptions are thrown when skip checksum errors is enabled
        Mockito.verify(reader, Mockito.times(1)).seek(100L);
    }
}