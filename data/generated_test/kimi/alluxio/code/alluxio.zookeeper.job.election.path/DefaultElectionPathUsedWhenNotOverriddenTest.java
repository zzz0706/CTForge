package alluxio.master;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.master.MasterInquireClient.Factory;
import alluxio.security.user.UserState;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ZkMasterInquireClient.class})
public class DefaultElectionPathUsedWhenNotOverriddenTest {

  @Test
  public void testDefaultElectionPathUsedWhenNotOverridden() throws Exception {
    // 1. Create a fresh configuration without overrides
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Compute expected value from the default
    String expectedElectionPath = conf.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH);

    // 3. Mock static dependencies
    mockStatic(ZkMasterInquireClient.class);
    UserState userStateMock = mock(UserState.class);

    // Stub the static call to return a dummy client
    when(ZkMasterInquireClient.getClient(
        anyString(), anyString(), anyString(), anyInt(), anyBoolean()))
        .thenReturn(mock(ZkMasterInquireClient.class));

    // 4. Enable ZooKeeper so the ZK path is used
    conf.set(PropertyKey.ZOOKEEPER_ENABLED, true);
    // Set a dummy zookeeper address to avoid RuntimeException
    conf.set(PropertyKey.ZOOKEEPER_ADDRESS, "localhost:2181");

    // 5. Invoke the method under test
    Factory.createForJobMaster(conf, userStateMock);

    // 6. Verify the election path argument equals the default
    verifyStatic();
    ZkMasterInquireClient.getClient(
        anyString(),
        eq(expectedElectionPath),
        anyString(),
        anyInt(),
        anyBoolean());

    assertEquals("/job_election", expectedElectionPath);
  }
}