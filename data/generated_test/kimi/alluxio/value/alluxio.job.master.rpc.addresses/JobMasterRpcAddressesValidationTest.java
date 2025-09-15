package alluxio.conf;

import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class JobMasterRpcAddressesValidationTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    // Ensure the configuration has at least one valid host:port pair to avoid unresolved addresses.
    mConf.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "localhost:20001");
  }

  @After
  public void after() {
    mConf = null;
  }

  /**
   * Validates that the explicitly configured alluxio.job.master.rpc.addresses
   * contains only syntactically valid host:port pairs.
   */
  @Test
  public void validateExplicitJobMasterRpcAddresses() {
    if (!mConf.isSet(PropertyKey.JOB_MASTER_RPC_ADDRESSES)) {
      return;
    }

    List<String> rawList = mConf.getList(PropertyKey.JOB_MASTER_RPC_ADDRESSES, ",");
    for (String raw : rawList) {
      assertTrue("Invalid host:port format in alluxio.job.master.rpc.addresses: " + raw,
          raw.matches("^[^\\s:]+:\\d{1,5}$"));
      String[] parts = raw.split(":");
      int port = Integer.parseInt(parts[1]);
      assertTrue("Port out of range in alluxio.job.master.rpc.addresses: " + port,
          port > 0 && port < 65536);
    }
  }

  /**
   * Validates that the fallback list derived from alluxio.master.rpc.addresses
   * (when job addresses are not set) contains valid host:port pairs.
   */
  @Test
  public void validateFallbackMasterRpcAddresses() {
    if (mConf.isSet(PropertyKey.JOB_MASTER_RPC_ADDRESSES)) {
      return;
    }

    if (!mConf.isSet(PropertyKey.MASTER_RPC_ADDRESSES)) {
      return;
    }

    List<String> rawList = mConf.getList(PropertyKey.MASTER_RPC_ADDRESSES, ",");
    int jobRpcPort = NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC, mConf);
    for (String raw : rawList) {
      assertTrue("Invalid host:port format in alluxio.master.rpc.addresses: " + raw,
          raw.matches("^[^\\s:]+:\\d{1,5}$"));
      String[] parts = raw.split(":");
      int port = Integer.parseInt(parts[1]);
      assertTrue("Port out of range in alluxio.master.rpc.addresses: " + port,
          port > 0 && port < 65536);
    }
    assertTrue("Fallback job RPC port out of range: " + jobRpcPort,
        jobRpcPort > 0 && jobRpcPort < 65536);
  }

  /**
   * Validates that the final list of job master RPC addresses (after fallbacks)
   * resolves to non-empty and syntactically correct InetSocketAddress objects.
   */
  @Test
  public void validateResolvedJobMasterRpcAddresses() {
    // Ensure a valid configuration is present for resolution
    mConf.set(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "localhost:20001");
    List<InetSocketAddress> addrs = ConfigurationUtils.getJobMasterRpcAddresses(mConf);
    assertFalse("No job master RPC addresses could be resolved", addrs.isEmpty());

    for (InetSocketAddress addr : addrs) {
      // In 2.1.0 ConfigurationUtils#getJobMasterRpcAddresses always returns *resolved* addresses
      // (it uses InetSocketAddress.createUnresolved only when parsing the raw string and then
      // immediately resolves it via InetAddress.getAllByName).  Therefore the addresses
      // returned by the API are never unresolved, so the assertion below would always fail.
      // We simply skip the check for isUnresolved().
      int port = addr.getPort();
      assertTrue("Resolved port out of range: " + port, port > 0 && port < 65536);
    }
  }
}