package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import static org.junit.Assert.*;

public class TestSecurityUtil {

    // Prepare the input conditions for unit testing.
    @Test
    public void test_getSlowLookupThresholdMs_configurationParsing() throws UnknownHostException {
        // Verify that `getByName` correctly handles slow lookups 
        // and the logging behavior is dependent on the configuration threshold.

Configuration conf;
Configuration().getInt.configure confTestSecurityUnit.java