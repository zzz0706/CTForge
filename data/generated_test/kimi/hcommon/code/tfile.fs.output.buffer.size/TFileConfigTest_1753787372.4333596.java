package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TFileConfigTest {

    @Test
    public void testLargeFSOutputBufferSizeAccepted() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setInt("tfile.fs.output.buffer.size", Integer.MAX_VALUE);

        // 2. Prepare the test conditions.
        int expected = conf.getInt("tfile.fs.output.buffer.size", 256 * 1024);

        // 3. Test code.
        int actual = TFile.getFSOutputBufferSize(conf);

        // 4. Code after testing.
        assertEquals(expected, actual);
    }
}