package alluxio.conf;

import alluxio.conf.PropertyKey;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;

public class ZookeeperJobElectionPathConfigValidationTest {

  @Test
  public void validateZookeeperJobElectionPath() {
    // 1. Use the Alluxio 2.1.0 API to obtain configuration values.
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    String electionPath = conf.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH);

    // 2. Prepare test conditions: if ZOOKEEPER_ENABLED is false, this property is ignored.
    boolean zkEnabled = conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
    if (!zkEnabled) {
      // When ZooKeeper is not enabled, any value (even invalid) is acceptable.
      return;
    }

    // 3. Test code: validate the election path.
    // Constraint: must be a non-empty absolute path (starts with '/').
    Assert.assertNotNull("ZOOKEEPER_JOB_ELECTION_PATH must not be null when ZooKeeper is enabled",
        electionPath);
    Assert.assertTrue("ZOOKEEPER_JOB_ELECTION_PATH must be an absolute path starting with '/'",
        electionPath.startsWith("/"));
    Assert.assertTrue("ZOOKEEPER_JOB_ELECTION_PATH must not be empty after the leading '/'",
        electionPath.length() > 1);
  }
}