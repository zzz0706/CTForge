package org.apache.hadoop.io.file.tfile;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class BCFileNegativeBufferSizeTest {

  @Test
  public void negativeBufferSizeIsPropagated() throws IOException {
    // 1. Use the hadoop-common 2.8.5 API to obtain configuration value
    Configuration conf = new Configuration();
    conf.setInt("tfile.fs.input.buffer.size", -1);

    // 2. Prepare the test conditions: simply call the static helper
    int bufferSize = TFile.getFSInputBufferSize(conf);

    // 3. Test code: assert the negative value is propagated
    assertEquals("Buffer size should be propagated exactly as configured (-1)",
                 -1, bufferSize);

    // 4. Nothing to clean up
  }
}