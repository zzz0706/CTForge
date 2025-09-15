package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterInquireClient.Factory;
import alluxio.security.user.UserState;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ZkMasterInquireClient.class, ConfigurationUtils.class})
public class CustomElectionPathOverridesDefaultTest {

    @Test
    public void testCustomElectionPathOverridesDefault() throws Exception {
        // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
        conf.set(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH, "/custom_job_election");
        conf.set(PropertyKey.ZOOKEEPER_ENABLED, true);
        conf.set(PropertyKey.ZOOKEEPER_ADDRESS, "localhost:2181");
        conf.set(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH, "/job_leader");
        conf.set(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT, 3);
        conf.set(PropertyKey.ZOOKEEPER_AUTH_ENABLED, false);

        // 2. Prepare the test conditions.
        UserState userStateMock = mock(UserState.class);
        PowerMockito.mockStatic(ConfigurationUtils.class);
        when(ConfigurationUtils.getJobMasterRpcAddresses(any())).thenReturn(null);

        PowerMockito.mockStatic(ZkMasterInquireClient.class);
        when(ZkMasterInquireClient.getClient(
                any(), any(), any(), anyInt(), anyBoolean()))
                .thenReturn(mock(ZkMasterInquireClient.class));

        // 3. Test code.
        MasterInquireClient client = Factory.createForJobMaster(conf, userStateMock);
        assertNotNull(client);

        // 4. Code after testing.
        PowerMockito.verifyStatic();
        ZkMasterInquireClient.getClient(
                any(), eq("/custom_job_election"), any(), anyInt(), anyBoolean());
    }
}