package alluxio.conf;

import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for validating {@code alluxio.job.master.embedded.journal.addresses}.
 * This test does NOT set any configuration value; it only validates what is read
 * from the current configuration.
 */
public class JobMasterEmbeddedJournalAddressesValidationTest {

  private AlluxioConfiguration mConf;

  @Before
  public void before() {
    // Ensure we are using the global configuration as-is (no overrides in the test)
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @After
  public void after() {
    // No-op: do not mutate global configuration
  }

  /**
   * Tests that the value read from the configuration file is syntactically valid
   * when the key is explicitly set.
   */
  @Test
  public void explicitValueValid() {
    if (!mConf.isSet(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES)) {
      return; // Nothing to validate
    }
    List<String> rawAddresses = mConf.getList(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, ",");
    for (String raw : rawAddresses) {
      try {
        HostAndPort hp = HostAndPort.fromString(raw.trim());
        assertNotNull("Missing hostname in " + raw, hp.getHost());
        assertTrue("Invalid port in " + raw, hp.hasPort() && hp.getPort() > 0 && hp.getPort() <= 65535);
      } catch (IllegalArgumentException e) {
        fail("Malformed address: " + raw);
      }
    }
  }

  /**
   * Verifies that the resolved list of {@link InetSocketAddress} objects is
   * non-empty and each entry has a valid port.
   */
  @Test
  public void resolvedAddressesValid() {
    List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterEmbeddedJournalAddresses(mConf);
    assertFalse("No job-master embedded journal addresses resolved", addresses.isEmpty());
    for (InetSocketAddress addr : addresses) {
      assertNotNull("Null address", addr);
      assertTrue("Invalid port: " + addr.getPort(),
          addr.getPort() > 0 && addr.getPort() <= 65535);
    }
  }

  /**
   * Ensures that if {@code alluxio.job.master.embedded.journal.addresses} is NOT
   * set, the fallback mechanism produces a valid port.  This indirectly checks
   * that {@code alluxio.master.embedded.journal.addresses} or the default
   * {@code JOB_MASTER_RAFT} service address is valid.
   */
  @Test
  public void fallbackPortValid() {
    if (mConf.isSet(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES)) {
      return; // No fallback used
    }
    int port = NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.JOB_MASTER_RAFT, mConf);
    assertTrue("Fallback job-master raft port invalid: " + port,
        port > 0 && port <= 65535);
  }
}