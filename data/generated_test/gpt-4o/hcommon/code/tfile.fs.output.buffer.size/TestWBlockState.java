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
 
        Configuration conf = new Configuration();
        int bufferSize = TFile.getFSOutputBufferSize(conf);

        ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
        FSDataOutputStream fsOut = new FSDataOutputStream(byteArrayOutStream, null);

        String compressionName = "gz"; 
        String comparatorName = "memcmp"; 


        Writer wBlockState = new Writer(fsOut, bufferSize, compressionName, comparatorName, conf);

        assertNotNull("Buffer State should not be null", wBlockState);
    }
}