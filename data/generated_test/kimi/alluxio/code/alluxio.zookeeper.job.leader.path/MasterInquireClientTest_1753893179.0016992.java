package alluxio.master;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.user.UserState;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ZkMasterInquireClient.class})
public class MasterInquireClientTest {

  @Test
  public void testZkDisabledIgnoresJobLeaderPath() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    InstancedConfiguration conf = new InstancedConfiguration(
        alluxio.conf.ServerConfiguration.global().copyProperties());
    conf.set(PropertyKey.ZOOKEEPER_ENABLED, false);

    // 2. Prepare the test conditions.
    mockStatic(ZkMasterInquireClient.class);
    Mockito.when(ZkMasterInquireClient.getClient(
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyInt(),
        Mockito.anyBoolean()))
        .thenThrow(new AssertionError("ZkMasterInquireClient should not be called"));

    UserState mockUserState = Mockito.mock(UserState.class);

    // 3. Test code.
    MasterInquireClient client =
        MasterInquireClient.Factory.createForJobMaster(conf, mockUserState);

    // 4. Code after testing.
    verifyStatic(never());
    ZkMasterInquireClient.getClient(
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyInt(),
        Mockito.anyBoolean());

    assertTrue(
        "Expected non-Zk client when Zookeeper is disabled",
        !(client instanceof ZkMasterInquireClient));
  }
}