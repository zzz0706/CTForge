package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsDfIntervalConfig {

  @Test
  public void testFsDfIntervalValidRange() {
    Configuration conf = new Configuration(false);
    conf.addResource("core-site.xml");

    long interval = conf.getLong(
        CommonConfigurationKeysPublic.FS_DF_INTERVAL_KEY,
        CommonConfigurationKeysPublic.FS_DF_INTERVAL_DEFAULT);

    // interval must be non-negative
    assertTrue("fs.df.interval must be >= 0, but was " + interval,
               interval >= 0);
  }
}