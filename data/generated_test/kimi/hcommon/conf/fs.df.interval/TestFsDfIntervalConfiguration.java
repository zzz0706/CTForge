package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsDfIntervalConfiguration {

  @Test
  public void testFsDfIntervalValidNonNegative() {
    Configuration conf = new Configuration();
    long interval = conf.getLong(CommonConfigurationKeysPublic.FS_DF_INTERVAL_KEY, 60000L);
    assertTrue("fs.df.interval must be >= 0", interval >= 0);
  }

  @Test
  public void testFsDfIntervalTypeIsLong() {
    Configuration conf = new Configuration();
    // Attempt to read as long; any ClassCastException indicates wrong type
    try {
      conf.getLong(CommonConfigurationKeysPublic.FS_DF_INTERVAL_KEY, 60000L);
    } catch (ClassCastException e) {
      fail("fs.df.interval must be convertible to long");
    }
  }
}