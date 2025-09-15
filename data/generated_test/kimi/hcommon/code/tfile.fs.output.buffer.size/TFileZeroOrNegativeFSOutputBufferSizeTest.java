package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TFileZeroOrNegativeFSOutputBufferSizeTest {

    @Test
    public void testZeroOrNegativeFSOutputBufferSizeHandledGracefully() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // Test with 0
        conf.setInt("tfile.fs.output.buffer.size", 0);
        int expectedZero = TFile.getFSOutputBufferSize(conf);
        assertEquals(0, expectedZero);

        // Test with -1
        conf.setInt("tfile.fs.output.buffer.size", -1);
        int expectedNeg = TFile.getFSOutputBufferSize(conf);
        assertEquals(-1, expectedNeg);
    }
}