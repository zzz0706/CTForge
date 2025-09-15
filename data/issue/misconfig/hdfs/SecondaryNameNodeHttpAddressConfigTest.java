package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

//hdfs-7359
public class SecondaryNameNodeHttpAddressConfigTest {

    private static final String KEY = "dfs.namenode.secondary.http-address";
    // HDFS secondary http-address
    private static final String DEFAULT_ADDRESS = "0.0.0.0:50090";

    private boolean isValidIpPort(String address) {
        if (address == null || address.indexOf(':') < 0) {
            return false;
        }
        String[] parts = address.split(":", 2);
        String ipPart = parts[0];
        String portPart = parts[1];

        try {
            InetAddress.getByName(ipPart);
        } catch (UnknownHostException e) {
            return false;
        }

        try {
            int port = Integer.parseInt(portPart);
            return port >= 1 && port <= 65535;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Test
    public void testSecondaryNamenodeHttpAddressFormat() {
        // Configuration conf = new Configuration();
        Configuration conf = new HdfsConfiguration();
        String address = conf.get(KEY, DEFAULT_ADDRESS);
        assertTrue(
                "Invalid value for " + KEY + ": '" + address
                        + "'. Expected IP:port format with a port range of 1-65535.",
                isValidIpPort(address));
    }
}
