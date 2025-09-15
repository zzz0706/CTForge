package org.apache.hadoop.hdfs.nfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

//HDFS-6455
public class NfsExportsAllowedHostsConfigTest {

    @Test
    public void testAllowedHostsFormat() {
        Configuration conf = new Configuration();
        String allowedHosts = conf.get("dfs.nfs.exports.allowed.hosts", null);

        if (allowedHosts == null || allowedHosts.trim().isEmpty()) {
            return;
        }

        String validPattern = "^[\\w\\d_\\-\\.\\=\\,\\s\\*]+$";
        assertTrue(
            "dfs.nfs.exports.allowed.hosts contains illegal characters or format: " + allowedHosts,
            allowedHosts.matches(validPattern)
        );
    }
}
