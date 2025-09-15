package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TFileConfigTest {

    @Test
    public void testDefaultFSOutputBufferSizeIsUsedWhenPropertyNotSet() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        long expectedBufferSize = conf.getInt("tfile.fs.output.buffer.size", 256 * 1024);

        // 2. Prepare the test conditions.
        BytesWritable mockFsOutputBuffer = mock(BytesWritable.class);
        FSDataOutputStream mockFsOut = mock(FSDataOutputStream.class);
        when(mockFsOut.getPos()).thenReturn(0L);

        // 3. Test code.
        int actualBufferSize = TFile.getFSOutputBufferSize(conf);

        // 4. Code after testing.
        assertEquals(expectedBufferSize, actualBufferSize);
    }
}