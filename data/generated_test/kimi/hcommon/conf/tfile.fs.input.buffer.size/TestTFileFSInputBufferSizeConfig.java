package org.apache.hadoop.io.file.tfile;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestTFileFSInputBufferSizeConfig {

  @Test
  public void testValidFSInputBufferSize() {
    Configuration conf = new Configuration();
    // 1. Obtain the value from the configuration file, no hard-coding
    int bufferSize = TFile.getFSInputBufferSize(conf);

    // 2. Prepare the test conditions – none needed, we only read

    // 3. Test code – validate the value
    // Buffer size must be a positive integer (> 0)
    assertTrue("tfile.fs.input.buffer.size must be > 0", bufferSize > 0);

    // 4. Code after testing – none
  }
}