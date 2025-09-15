package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure.Policy;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestReplaceDatanodeOnFailurePolicyConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    // Ensure we do NOT set any values in code – rely only on loaded config files
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * Test that when dfs.client.block.write.replace-datanode-on-failure.enable
   * is true, the policy value must be one of {DEFAULT, ALWAYS, NEVER}.
   */
  @Test
  public void testPolicyValueWhenEnabled() {
    // 1. Load whatever is in the configuration files
    boolean enabled = conf.getBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT);

    if (enabled) {
      String policyStr = conf.get(
          HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
          HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT);

      boolean valid = false;
      for (Policy p : Policy.values()) {
        if (p != Policy.DISABLE && p.name().equalsIgnoreCase(policyStr)) {
          valid = true;
          break;
        }
      }
      assertTrue("Invalid policy value \"" + policyStr + "\" for " +
          HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY +
          " when " +
          HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY +
          " is true", valid);
    }
  }

  /**
   * Test that ReplaceDatanodeOnFailure#get throws HadoopIllegalArgumentException
   * for an illegal policy string when the feature is enabled.
   */
  @Test(expected = HadoopIllegalArgumentException.class)
  public void testIllegalPolicyThrowsException() {
    // Force-enable the feature
    conf.setBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        true);
    // Inject an invalid policy value
    conf.set(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
        "INVALID_POLICY");

    // Attempt to parse – should throw
    ReplaceDatanodeOnFailure.get(conf);
  }
}