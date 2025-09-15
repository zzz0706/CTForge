package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.*;

import java.net.URI;
//hadoop-10049 hadoop-2358 hadoop-13792
public class TestFsDefaultFSConfiguration {

    @Test
    public void testFsDefaultFSConfigurationValidity() {
        // Load Hadoop configuration file
        Configuration conf = new Configuration();

        // Retrieve fs.defaultFS configuration value
        String defaultFS = conf.get("fs.defaultFS", "file:///");

        // Ensure the value is not null or empty
        assertNotNull("fs.defaultFS should not be null", defaultFS);
        assertFalse("fs.defaultFS should not be empty", defaultFS.trim().isEmpty());

        // Validate that the value is a valid URI
        try {
            URI defaultFSUri = URI.create(defaultFS);
            assertNotNull("fs.defaultFS should be a valid URI", defaultFSUri);
            assertNotNull("fs.defaultFS should have a scheme", defaultFSUri.getScheme());
        } catch (IllegalArgumentException e) {
            fail("fs.defaultFS is not a valid URI: " + e.getMessage());
        }
    }

    @Test
    public void testFsDefaultFSDependency() {
        // Load Hadoop configuration file
        Configuration conf = new Configuration();

        // Retrieve fs.defaultFS and dfs.namenode.rpc-address configuration values
        String defaultFS = conf.get("fs.defaultFS");
        String nameNodeRpcAddress = conf.get("dfs.namenode.rpc-address");

        // If fs.defaultFS is not set, it should default to dfs.namenode.rpc-address
        if (defaultFS == null || defaultFS.trim().isEmpty()) {
            assertNotNull("fs.defaultFS is unset, so dfs.namenode.rpc-address should be set", nameNodeRpcAddress);
        }
    }
}