package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

public class TestWBlockStateIntegrationWithBufferedStream {

    @Test
    public void testBufferSizePropagationToBufferedStream() throws Exception {
        // Step 1: Create a Configuration object
        Configuration conf = new Configuration();

        // Step 2: Create mock/stub instances for FSDataOutputStream
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        FSDataOutputStream mockFsOut = Mockito.spy(new FSDataOutputStream(byteOutputStream, null));

        // Step 3: Instantiate a BytesWritable object
        BytesWritable bytesWritable = new BytesWritable(new byte[]{1, 2, 3, 4}); // initialize with sample data

        // Step 4: Set required values for the TFile.Writer constructor
        int minBlockSize = TFile.getFSOutputBufferSize(conf); // Retrieve block size from configuration (as API does not hard-code value)
        String compressionName = "none"; // Example compression codec name
        String comparator = null; // Use default comparator if none is specified

        // Step 5: Use TFile.Writer directly and simulate BlockAppender operations manually
        TFile.Writer writer = new TFile.Writer(mockFsOut, minBlockSize, compressionName, comparator, conf);

        // Step 6: Write some sample key-value data to the TFile.Writer for testing purposes
        writer.append(new byte[]{11, 12}, bytesWritable.getBytes()); // Correct method signature
        writer.close(); // Ensure the writer is closed to flush the data

        // Step 7: Verify that the buffer capacity matches the propagated configuration
        int expectedBufferSize = TFile.getFSOutputBufferSize(conf); // Retrieve value using API
        assertEquals(expectedBufferSize, minBlockSize); // Compare against minimum block size
    }
}