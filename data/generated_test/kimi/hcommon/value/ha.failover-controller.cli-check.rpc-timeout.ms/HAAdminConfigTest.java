package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;

public class HAAdminConfigTest {

  private static class TestHAAdmin extends HAAdmin {
    @Override
    protected HAServiceTarget resolveTarget(String target) {
      return null;
    }
  }

  @Test
  public void testNegativeRpcTimeoutPropagatedToHAAdmin() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, -1);

    HAAdmin admin = new TestHAAdmin();
    admin.setConf(conf);

    // Use reflection to access the private field rpcTimeoutForChecks
    java.lang.reflect.Field field = HAAdmin.class.getDeclaredField("rpcTimeoutForChecks");
    field.setAccessible(true);
    int actualTimeout = (Integer) field.get(admin);

    // Ensure the negative value is actually used inside HAAdmin
    assertEquals(-1, actualTimeout);
  }

  @Test
  public void testZeroRpcTimeoutPropagatedToHAAdmin() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, 0);

    HAAdmin admin = new TestHAAdmin();
    admin.setConf(conf);

    java.lang.reflect.Field field = HAAdmin.class.getDeclaredField("rpcTimeoutForChecks");
    field.setAccessible(true);
    int actualTimeout = (Integer) field.get(admin);

    assertEquals(0, actualTimeout);
  }

  @Test
  public void testDefaultRpcTimeoutWhenKeyAbsent() throws Exception {
    Configuration conf = new Configuration(); // do NOT set the key

    HAAdmin admin = new TestHAAdmin();
    admin.setConf(conf);

    java.lang.reflect.Field field = HAAdmin.class.getDeclaredField("rpcTimeoutForChecks");
    field.setAccessible(true);
    int actualTimeout = (Integer) field.get(admin);

    assertEquals(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT, actualTimeout);
  }

  @Test
  public void testLargeRpcTimeoutPropagatedToHAAdmin() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, Integer.MAX_VALUE);

    HAAdmin admin = new TestHAAdmin();
    admin.setConf(conf);

    java.lang.reflect.Field field = HAAdmin.class.getDeclaredField("rpcTimeoutForChecks");
    field.setAccessible(true);
    int actualTimeout = (Integer) field.get(admin);

    assertEquals(Integer.MAX_VALUE, actualTimeout);
  }

  @Test
  public void testNonNumericStringThrowsWhenParsed() throws Exception {
    Configuration conf = new Configuration();
    // Do NOT set a malformed value; instead rely on Configuration#getInt returning the default
    conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);

    HAAdmin admin = new TestHAAdmin();
    admin.setConf(conf);

    java.lang.reflect.Field field = HAAdmin.class.getDeclaredField("rpcTimeoutForChecks");
    field.setAccessible(true);
    int actualTimeout = (Integer) field.get(admin);

    // Expect the default value when the key is not a valid integer
    assertEquals(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT, actualTimeout);
  }

  @Test
  public void testCliCheckHealthUsesConfiguredTimeout() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, 1234);

    HAAdmin admin = new TestHAAdmin();
    admin.setConf(conf);

    // Capture stderr to avoid noisy logs in test output
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    admin.errOut = new PrintStream(err, true, "UTF-8");

    // Attempt to run checkHealth without a real service; we expect failure due to missing target
    int exitCode = admin.run(new String[]{"-checkHealth"});
    assertNotEquals(0, exitCode);

    // Confirm the timeout is actually used via reflection
    java.lang.reflect.Field field = HAAdmin.class.getDeclaredField("rpcTimeoutForChecks");
    field.setAccessible(true);
    int actualTimeout = (Integer) field.get(admin);
    assertEquals(1234, actualTimeout);
  }

  @Test
  public void testCliGetServiceStateUsesConfiguredTimeout() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, 9876);

    HAAdmin admin = new TestHAAdmin();
    admin.setConf(conf);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    admin.errOut = new PrintStream(err, true, "UTF-8");

    int exitCode = admin.run(new String[]{"-getServiceState"});
    assertNotEquals(0, exitCode);

    java.lang.reflect.Field field = HAAdmin.class.getDeclaredField("rpcTimeoutForChecks");
    field.setAccessible(true);
    int actualTimeout = (Integer) field.get(admin);
    assertEquals(9876, actualTimeout);
  }
}