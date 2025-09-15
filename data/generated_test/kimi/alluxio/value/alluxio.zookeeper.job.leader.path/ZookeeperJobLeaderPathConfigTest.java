package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient;
import alluxio.security.user.UserState;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ZookeeperJobLeaderPathConfigTest {

  @Test
  public void testValidJobLeaderPath() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = InstancedConfiguration.defaults();

    // 2. Prepare the test conditions.
    if (!conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      return;
    }

    // 3. Test code.
    String leaderPath = conf.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);

    // 4. Code after testing.
    assertTrue("alluxio.zookeeper.job.leader.path must not be empty",
        leaderPath != null && !leaderPath.trim().isEmpty());
    assertTrue("alluxio.zookeeper.job.leader.path must start with '/'",
        leaderPath.startsWith("/"));
  }

  @Test
  public void testJobLeaderPathUsedInZkClientCreation() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    UserState userState = UserState.Factory.create(conf);

    // 2. Prepare the test conditions.
    if (!conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      return;
    }

    // 3. Test code.
    MasterInquireClient client = MasterInquireClient.Factory.createForJobMaster(conf, userState);

    // 4. Code after testing.
    assertTrue("ZK-based MasterInquireClient should be created when ZK is enabled",
        client instanceof alluxio.master.ZkMasterInquireClient);
  }

  @Test
  public void testJobLeaderPathIgnoredWhenZkDisabled() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    UserState userState = UserState.Factory.create(conf);

    // 2. Prepare the test conditions.
    if (conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      return;
    }

    // 3. Test code.
    List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);
    MasterInquireClient client = MasterInquireClient.Factory.createForJobMaster(conf, userState);

    // 4. Code after testing.
    if (addresses.size() > 1) {
      assertTrue("PollingMasterInquireClient should be used when ZK is disabled and >1 address",
          client instanceof alluxio.master.PollingMasterInquireClient);
    } else {
      assertTrue("SingleMasterInquireClient should be used when ZK is disabled and 1 address",
          client instanceof alluxio.master.SingleMasterInquireClient);
    }
  }
}