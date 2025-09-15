package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMapFileIndexIntervalConfig {

  @Test
  public void testIndexIntervalPositive() throws Exception {
    Configuration conf = new Configuration();
    // 1. read value from conf, do NOT set it in test
    int interval = conf.getInt("io.map.index.interval", 128);

    // 2. assert it is a positive int (code uses it as a divisor & step)
    assertTrue("io.map.index.interval must be > 0", interval > 0);
  }

  @Test
  public void testIndexIntervalIsInt() throws Exception {
    Configuration conf = new Configuration();
    // 1. try to fetch as int; if it throws, the value is malformed
    try {
      conf.getInt("io.map.index.interval", 128);
    } catch (NumberFormatException nfe) {
      fail("io.map.index.interval must be a valid integer");
    }
  }
}