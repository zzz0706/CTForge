package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestDFSConfiguration {

    /**
     * Test to validate the `dfs.namenode.hosts.provider.classname` configuration.
     */
    @Test
    public void testDfsNamenodeHostsProviderClassnameConfiguration() {
        // Step 1: Use the Configuration API to fetch the value of the configuration key
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY, HostFileManager.class.getName());
        String configuredValue = conf.get(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY);

        // Step 2: Define valid classes.
        Class<?> defaultClass = HostFileManager.class;
        Class<?> alternativeClass = CombinedHostFileManager.class;

        try {
            // Step 3: Validate the configured value and check if it corresponds to a valid class.
            Class<?> configuredClass = conf.getClassByName(configuredValue.trim());

            // Ensure the configured class matches either the default or alternative class.
            Assert.assertTrue("The configured class '" + configuredValue
                    + "' must be either '" + defaultClass.getName()
                    + "' or '" + alternativeClass.getName() + "'.", 
                    defaultClass.isAssignableFrom(configuredClass) || 
                    alternativeClass.isAssignableFrom(configuredClass));

            // Step 4: Instantiate the HostConfigManager and confirm its type compatibility.
            Object instance = ReflectionUtils.newInstance(configuredClass, conf);

            Assert.assertNotNull("Failed to instantiate the configured class: " + configuredValue, instance);
            Assert.assertTrue("Configured class instance is not of type HostConfigManager.", 
                    HostConfigManager.class.isAssignableFrom(configuredClass));

        } catch (ClassNotFoundException e) {
            Assert.fail("The configured class value '" + configuredValue + "' cannot be resolved: " + e.getMessage());
        }
    }
}