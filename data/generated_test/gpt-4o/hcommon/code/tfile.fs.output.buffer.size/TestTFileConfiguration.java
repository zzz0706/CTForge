package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.file.tfile.TFile;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestTFileConfiguration {
    
    // Prepare the input conditions for unit testing.
    @Test
    public void testGetFSOutputBufferSizeWithDefaultValue() {
        // Create a new Configuration object with no specific key set.
        Configuration conf = new Configuration();

        // Get buffer size using the API.
        int bufferSize = TFile.getFSOutputBufferSize(conf);

        // Verify buffer size is equal to the default value, which is 256 KB.
        assertTrue("Buffer size should be equal to the default value of 256 KB", bufferSize == 256 * 1024);
    }
}