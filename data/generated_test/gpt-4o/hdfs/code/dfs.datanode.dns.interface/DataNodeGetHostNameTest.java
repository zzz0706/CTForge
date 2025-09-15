package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

public class DataNodeGetHostNameTest {

    private Configuration config;

    @Before
    public void setUp() {
        // Prepare the Configuration object
        // 1. Using the correct HDFS 2.8.5 API to ensure valid configuration setup.
        config = new Configuration();

        // 2. Make sure DFS_DATANODE_DNS_INTERFACE_KEY and DFS_DATANODE_DNS_NAMESERVER_KEY are not set to simulate missing DNS settings.
        config.unset(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY);
        config.unset(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY);

        // 3. Ensure HADOOP_SECURITY_DNS_INTERFACE_KEY is not set for security-related configurations.
        config.unset(DFSConfigKeys.HADOOP_SECURITY_DNS_INTERFACE_KEY);

        // 4. Use default fallback values to emulate missing configurations in the DataNode class logic.
        config.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "default-hostname");
    }

    @Test
    public void testDnsInterfaceNullLegacyConfigMissing() {
        // Handling reflection and invoking the private method `getHostName` in the DataNode class
        try {
            // Access `getHostName` using reflection 
            // Ensure that the DataNode class is using the correct configuration keys
            Method getHostNameMethod = DataNode.class.getDeclaredMethod("getHostName", Configuration.class);
            getHostNameMethod.setAccessible(true);

            try {
                // 1. Invoke the method with the configuration that simulates missing DNS settings
                Object result = getHostNameMethod.invoke(null, config);

                // 2. Verify that a valid hostname is returned when legacy configuration is missing
                assertTrue("Valid hostname should be returned when default configuration is set", result instanceof String);

            } catch (InvocationTargetException e) {
                // 3. Check if the root cause of the exception is UnknownHostException
                if (e.getCause() instanceof UnknownHostException) {
                    // UnknownHostException is thrown as expected in case of invalid configuration
                    fail("UnknownHostException should not occur with fallback configuration: " + e.getCause().getMessage());
                } else {
                    // Re-throw the exception if it is not of type UnknownHostException
                    throw e;
                }
            }
        } catch (NoSuchMethodException e) {
            // Fail the test if the `getHostName` method could not be found
            fail("The getHostName method does not exist in the DataNode class: " + e.getMessage());
        } catch (Exception e) {
            // Fail the test if any unexpected exception occurs
            fail("Unexpected exception occurred: " + e.getMessage());
        }
    }
}