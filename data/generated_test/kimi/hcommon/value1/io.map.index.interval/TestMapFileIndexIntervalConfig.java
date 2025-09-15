package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMapFileIndexIntervalConfig {

  @Test
  public void testIndexIntervalValidRange() {
    Configuration conf = new Configuration(false);
    // Do NOT set the value in code – read only
    int interval = conf.getInt("io.map.index.interval", 128);

    // io.map.index.interval must be a positive integer
    assertTrue("io.map.index.interval must be > 0", interval > 0);
  }
}