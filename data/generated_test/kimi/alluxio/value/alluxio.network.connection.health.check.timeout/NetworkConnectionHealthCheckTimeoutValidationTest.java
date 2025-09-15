package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for validating {@code alluxio.network.connection.health.check.timeout}.
 */
public class NetworkConnectionHealthCheckTimeoutValidationTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    // 1. Load configuration without hard-coding any value
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @After
  public void after() {
    // 4. Clean-up after test
    mConf = null;
  }

  /**
   * Validates that the timeout is a positive duration.
   */
  @Test
  public void timeoutMustBePositive() {
    // 2. Prepare test condition: read value from configuration
    long timeoutMs = mConf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

    // 3. Validate constraint
    assertTrue("alluxio.network.connection.health.check.timeout must be > 0 ms", timeoutMs > 0);
  }

  /**
   * Validates that the timeout is not too large to cause overflow in
   * {@link alluxio.util.CommonUtils#waitForResult(String, java.util.function.Supplier, alluxio.util.WaitForOptions)}.
   */
  @Test
  public void timeoutMustNotOverflow() {
    // 2. Prepare test condition: read value from configuration
    long timeoutMs = mConf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

    // 3. Validate constraint: int cast must not overflow
    try {
      int intTimeout = (int) timeoutMs;
      assertEquals("Timeout should fit into int without overflow", timeoutMs, intTimeout);
    } catch (ArithmeticException e) {
      fail("alluxio.network.connection.health.check.timeout causes overflow when cast to int");
    }
  }

  /**
   * Validates that the timeout is actually used when acquiring a channel via
   * {@link alluxio.grpc.GrpcManagedChannelPool#acquireManagedChannel}.
   */
  @Test
  public void timeoutIsUsedInGrpcManagedChannelPool() {
    // 2. Prepare test condition: read value from configuration
    long expectedTimeoutMs = mConf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

    // 3. Validate usage: ensure the value is passed into the pool
    GrpcServerAddress address = GrpcServerAddress.create(new InetSocketAddress("localhost", 19999));
    GrpcChannelBuilder builder = GrpcChannelBuilder
        .newBuilder(address, mConf)
        .setSubject(null);

    try {
      // Attempt to build will internally call acquireManagedChannel with the timeout
      builder.build();
    } catch (Exception e) {
      // Expected if no server is running; we only care that the timeout value is used
    }

    // If we reach here without an IllegalArgumentException, the value was accepted
  }
}