package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.fail;

public class DataNodeTest {

    private Configuration config;

    @Before
    public void setUp() {
        // Prepare configuration settings
        // Using hdfs 2.8.5 API to correctly mock configuration values
        config = new Configuration();
        config.set("hadoop.security.dns.interface", "lo"); // Using loopback interface as an example
        config.set("hadoop.security.dns.nameserver", "127.0.0.1"); // Using localhost for DNS nameserver
    }

    @Test
    public void exceptionOnInvalidDnsSettings() throws UnknownHostException {
        try {
            // Prepare invalid DNS settings
            config.set("hadoop.security.dns.interface", "invalid_interface");
            config.set("hadoop.security.dns.nameserver", "invalid_server");

            // Attempt to resolve default DNS, expecting it to throw an exception
            String hostname = DNS.getDefaultHost(
                config.get("hadoop.security.dns.interface"),
                config.get("hadoop.security.dns.nameserver"),
                false
            );

            // Expecting failure
            fail("Expected UnknownHostException was not thrown. Resolved hostname: " + hostname);
        } catch (UnknownHostException e) {
            // Expected exception was caught
            // Test passes successfully
        }
    }
}