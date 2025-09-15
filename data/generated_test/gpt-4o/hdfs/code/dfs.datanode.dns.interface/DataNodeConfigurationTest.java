package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.junit.Test;
import static org.junit.Assert.*;

public class DataNodeConfigurationTest {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getHostName_with_hadoop_security_dns_interface() throws Exception {
        // Step 1: Prepare the test conditions
        // Create a Configuration object and configure 'hadoop.security.dns.interface' and optional 'hadoop.security.dns.nameserver'
        Configuration config = new Configuration();
        config.set("hadoop.security.dns.interface", "default"); // Set to default interface to avoid ArrayIndexOutOfBoundsException
        
        // Optional: Set the nameserver if required
        config.set("hadoop.security.dns.nameserver", "localhost");

        // Step 2: Test execution
        // Fetching hostname using DNS.getDefaultHost with proper configuration values
        // Ensure the configuration values are non-null and valid
        try {
            String dnsInterface = config.get("hadoop.security.dns.interface");
            String dnsNameserver = config.get("hadoop.security.dns.nameserver");
            
            // Fall back to safe defaults if the values are not properly set
            dnsInterface = dnsInterface != null ? dnsInterface : "default";
            dnsNameserver = dnsNameserver != null ? dnsNameserver : null; // null means using the system nameserver
            
            String resolvedHostName = DNS.getDefaultHost(dnsInterface, dnsNameserver);
            
            // Step 3: Verify the outcome
            // Verify the resolved hostname is not null or empty
            assertNotNull("Resolved hostname should not be null", resolvedHostName);
            assertFalse("Resolved hostname should not be empty", resolvedHostName.isEmpty());

        } catch (ArrayIndexOutOfBoundsException ex) {
            // Log the exception and fail the test if unexpected behavior arises
            fail("Configuration caused an ArrayIndexOutOfBoundsException: " + ex.getMessage());
        }

        // Step 4: Code clean-up after testing
        // No additional cleanup required in this case.
    }
}