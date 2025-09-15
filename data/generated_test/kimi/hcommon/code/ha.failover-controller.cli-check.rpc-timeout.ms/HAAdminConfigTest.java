package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HAAdminConfigTest {

  @Test
  public void CLI_RPC_timeout_negative_branch_behavior() throws Exception {
    // 1. Instantiate Configuration and set negative value
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, -1);

    // 2. Calculate expected value via conf.getInt
    int expectedTimeout = conf.getInt(
            CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
            CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);

    // 3. Verify the negative timeout is preserved
    assertEquals(-1, expectedTimeout);
  }
}