package org.apache.hadoop.security;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestHadoopRpcProtectionConfig {

  private static final Set<String> VALID_VALUES = new HashSet<>(
      Arrays.asList("authentication", "integrity", "privacy"));

  @Test
  public void testHadoopRpcProtectionValid() {
    Configuration conf = new Configuration();
    conf.addResource("core-site.xml");

    String[] protections = conf.getTrimmedStrings(
        CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION,
        "authentication");

    for (String p : protections) {
      String lower = p.toLowerCase();
      assertTrue("Invalid value for " + CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION
          + ": " + p, VALID_VALUES.contains(lower));
    }
  }
}