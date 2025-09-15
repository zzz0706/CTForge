package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

//HDFS-12109
public class HaFailoverProxyProviderConfigTest {

    @Test
    public void testFailoverProxyProviderConfig() {
        Configuration conf = new Configuration();
        String nameservices = conf.get("dfs.nameservices", null);

        // If nameservices not configured, nothing to check
        if (nameservices == null || nameservices.trim().isEmpty()) return;

        String[] nsList = nameservices.trim().split("\\s*,\\s*");
        for (String ns : nsList) {
            String key = "dfs.client.failover.proxy.provider." + ns;
            String value = conf.get(key, null);

            // Fail if the required property is missing or empty
            assertNotNull(
                "Missing required HA configuration: " + key,
                value
            );
            assertFalse(
                "HA configuration " + key + " must not be empty.",
                value.trim().isEmpty()
            );

            // Optionally, check value is a valid class name (basic regex)
            String classPattern = "^[a-zA-Z_$][a-zA-Z\\d_$]*(\\.[a-zA-Z_$][a-zA-Z\\d_$]*)*$";
            assertTrue(
                "Value of " + key + " is not a valid Java class name: " + value,
                value.trim().matches(classPattern)
            );
        }
    }
}
