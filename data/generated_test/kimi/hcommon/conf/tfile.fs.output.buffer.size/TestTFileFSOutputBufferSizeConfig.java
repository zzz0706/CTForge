package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestTFileFSOutputBufferSizeConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  @Test
  public void testValidFSOutputBufferSize() {
    // 1. Read the configured value
    int bufferSize = TFile.getFSOutputBufferSize(conf);

    // 2. Prepare the test conditions: none, we rely on the configuration file

    // 3. Test code: buffer size must be a positive integer
    assertTrue("tfile.fs.output.buffer.size must be a positive integer",
               bufferSize > 0);

    // 4. Code after testing: none
  }
}