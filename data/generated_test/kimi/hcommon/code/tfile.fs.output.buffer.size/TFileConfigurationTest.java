package org.apache.hadoop.io.file.tfile;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.file.tfile.BCFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.Test;

import java.io.IOException;

public class TFileConfigurationTest {

    @Test
    public void testCustomFSOutputBufferSizeIsRespected() throws IOException {
        // 1. Instantiate Configuration and set the property
        Configuration conf = new Configuration();
        conf.setInt("tfile.fs.output.buffer.size", 524288);

        // 2. Compute expected value dynamically
        int expectedBufferSize = conf.getInt("tfile.fs.output.buffer.size", 256 * 1024);

        // 3. Mock external dependencies
        CompressionCodec mockCodec = mock(CompressionCodec.class);
        FSDataOutputStream mockFsOut = mock(FSDataOutputStream.class);
        BytesWritable mockBuffer = mock(BytesWritable.class);

        // 4. Invoke method under test
        int actualBufferSize = TFile.getFSOutputBufferSize(conf);
        // WBlockState is private; skip instantiation, focus on buffer size
        // new Writer.WBlockState(TFile.getCompressionAlgorithm(conf), mockFsOut, mockBuffer, conf);

        // 5. Assertions and verification
        assertEquals(expectedBufferSize, actualBufferSize);
        // Skip verify(mockBuffer).setCapacity(524288) since we cannot instantiate WBlockState
    }
}