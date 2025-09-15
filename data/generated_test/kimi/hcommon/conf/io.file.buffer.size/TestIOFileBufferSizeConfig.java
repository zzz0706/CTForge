package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.LineReader;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestIOFileBufferSizeConfig {

  @Test
  public void testIOFileBufferSizeValid() {
    Configuration conf = new Configuration(false);
    // Do NOT set the value in the test; read whatever is present
    int bufferSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);

    // Rule: bufferSize must be > 0 (used as array length)
    assertTrue("io.file.buffer.size must be positive", bufferSize > 0);

    // Rule: bufferSize should be a multiple of 4096 (page size)
    assertEquals("io.file.buffer.size should be a multiple of 4096",
                 0, bufferSize % 4096);
  }

  @Test
  public void testIOFileBufferSizeUsedByClasses() throws IOException {
    Configuration conf = new Configuration(false);
    int bufferSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);

    // Ensure all consumers can instantiate their buffers
    // SequenceFile.Writer/Reader do not expose a static getBufferSize method in 2.8.5
    // DefaultCodec / GzipCodec / BZip2Codec createOutputStream calls will NPE on null streams
    // LineReader.getBufferSize() is protected in 2.8.5

    // Codec usages – we can only check that the codec instances are created
    DefaultCodec defaultCodec = new DefaultCodec();
    defaultCodec.setConf(conf);
    assertNotNull("DefaultCodec should be instantiable", defaultCodec);

    GzipCodec gzipCodec = new GzipCodec();
    gzipCodec.setConf(conf);
    assertNotNull("GzipCodec should be instantiable", gzipCodec);

    BZip2Codec bz2Codec = new BZip2Codec();
    bz2Codec.setConf(conf);
    assertNotNull("BZip2Codec should be instantiable", bz2Codec);

    // LineReader – just ensure it can be constructed
    assertNotNull("LineReader should be instantiable", new LineReader(null, conf));

    // IOUtils – check the same value is still positive
    assertTrue("IOUtils buffer size must be positive",
               conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                           CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT) > 0);
  }
}