package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class TestDeleteBlockPoolWithSuperuserPrivilege {

    private DataNode dataNode;
    private Configuration conf;
    private UserGroupInformation superuserUgi;

    @Before
    public void setUp() throws Exception {
        // Prepare the test configuration and set superuser group
        conf = new Configuration();
        String superuserGroup = conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY, 
             DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
        
        // Mock the DataNode and initialize it with the configuration
        dataNode = Mockito.mock(DataNode.class);
        Mockito.when(dataNode.getConf()).thenReturn(conf);

        // Simulate RPC call with a superuser
        superuserUgi = Mockito.mock(UserGroupInformation.class);
        Mockito.when(superuserUgi.getGroupNames()).thenReturn(new String[]{superuserGroup});
        Mockito.when(superuserUgi.getUserName()).thenReturn("testSuperUser");
    }

    @Test
    public void test_deleteBlockPool_withSuperuserPrivilege() throws IOException {
        // Prepare necessary configurations and mocks
        String blockPoolId = "testBlockPool";
        boolean forceDelete = true;

        // Simulate privileged operation
        Mockito.doNothing().when(dataNode).deleteBlockPool(Mockito.anyString(), Mockito.anyBoolean());

        // Invoke the deleteBlockPool method
        dataNode.deleteBlockPool(blockPoolId, forceDelete);

        // Verify that the block pool deletion operation was called successfully
        Mockito.verify(dataNode).deleteBlockPool(blockPoolId, forceDelete);
    }
}