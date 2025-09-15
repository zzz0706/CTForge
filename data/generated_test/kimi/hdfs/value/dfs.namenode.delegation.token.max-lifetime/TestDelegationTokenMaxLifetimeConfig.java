package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDelegationTokenMaxLifetimeConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // Prepare a fresh Configuration instance; no values are set here
    conf = new Configuration(false);
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  /**
   * Validates that the value provided for
   * dfs.namenode.delegation.token.max-lifetime is a positive long.
   *
   * The DelegationTokenSecretManager (and its superclass
   * AbstractDelegationTokenSecretManager) treat this value as the upper bound
   * for delegation-token lifetime.  A non-positive value would cause immediate
   * expiry of every newly issued token and must therefore be rejected.
   */
  @Test
  public void testMaxLifetimeIsPositiveLong() {
    // 1. Obtain the value from the configuration file (not set in test code)
    long maxLifetime = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);

    // 2 & 3. Validate the constraint
    assertTrue(
        "dfs.namenode.delegation.token.max-lifetime must be a positive long",
        maxLifetime > 0L);
  }
}