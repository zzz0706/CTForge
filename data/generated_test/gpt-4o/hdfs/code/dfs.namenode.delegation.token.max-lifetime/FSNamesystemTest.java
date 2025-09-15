package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class FSNamesystemTest {

    @Test
    public void testCreateDelegationTokenSecretManager_validConfiguration() throws Exception {
        // Step 1: Get configuration values using the API
        Configuration conf = new Configuration();
        long maxLifetime = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);

        // Ensure the configuration value is retrieved
        assertNotNull("Configuration value for dfs.namenode.delegation.token.max-lifetime should not be null.",
                maxLifetime);

        // Step 2: Prepare the input conditions for unit testing
        FSImage fsImage = new FSImage(conf); // FSImage required for FSNamesystem initialization
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage); // Proper constructor usage

        // Step 3: Use reflection to access the private createDelegationTokenSecretManager method
        java.lang.reflect.Method method = FSNamesystem.class.getDeclaredMethod("createDelegationTokenSecretManager",
                Configuration.class);
        method.setAccessible(true); // Make private method accessible
        
        Object dtSecretManager = method.invoke(fsNamesystem, conf); // Call the private method

        // Step 4: Assert that the DelegationTokenSecretManager is successfully instantiated
        assertNotNull("DelegationTokenSecretManager should be successfully created.", dtSecretManager);
    }
}