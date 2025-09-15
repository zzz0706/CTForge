package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient;
import alluxio.master.ZkMasterInquireClient;
import alluxio.security.user.UserState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MasterInquireClientTest {

    private AlluxioConfiguration mConfig;
    private UserState mUserState;

    // Prepare the test conditions
    @Before
    public void setUp() {
        mConfig = Mockito.mock(AlluxioConfiguration.class);
        mUserState = Mockito.mock(UserState.class);

        // Use the AlluxioConfiguration API to obtain configuration values instead of hardcoding them
        Mockito.when(mConfig.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)).thenReturn(true);
        Mockito.when(mConfig.get(PropertyKey.ZOOKEEPER_ADDRESS)).thenReturn("localhost:2181");
        Mockito.when(mConfig.get(PropertyKey.ZOOKEEPER_ELECTION_PATH)).thenReturn("/election_path");
        Mockito.when(mConfig.get(PropertyKey.ZOOKEEPER_LEADER_PATH)).thenReturn("/leader_path");
        Mockito.when(mConfig.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT)).thenReturn(3);
        Mockito.when(mConfig.getBoolean(PropertyKey.ZOOKEEPER_AUTH_ENABLED)).thenReturn(false);
    }

    // Test code
    @Test
    public void testCreateForZookeeperMaster() {
        // Invoke the create method for ZooKeeper Master
        MasterInquireClient client = MasterInquireClient.Factory.create(mConfig, mUserState);

        // Verify the returned instance type
        Assert.assertTrue(client instanceof ZkMasterInquireClient);

        // Verify that the initialization used the correct ZooKeeper configurations
        ZkMasterInquireClient zkClient = (ZkMasterInquireClient) client;
        Mockito.verify(mConfig).getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
        Mockito.verify(mConfig).get(PropertyKey.ZOOKEEPER_ADDRESS);
        Mockito.verify(mConfig).get(PropertyKey.ZOOKEEPER_ELECTION_PATH);
        Mockito.verify(mConfig).get(PropertyKey.ZOOKEEPER_LEADER_PATH);
        Mockito.verify(mConfig).getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT);
        Mockito.verify(mConfig).getBoolean(PropertyKey.ZOOKEEPER_AUTH_ENABLED);
    }
}