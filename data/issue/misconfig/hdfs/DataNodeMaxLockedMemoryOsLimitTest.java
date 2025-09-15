package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

//HDFS-5348
public class DataNodeMaxLockedMemoryOsLimitTest {

    @Test
    public void testMaxLockedMemoryIsPositive() {
        Configuration conf = new Configuration();
        long maxLockedMemory = conf.getLong("dfs.datanode.max.locked.memory", 0L);
        assertTrue("dfs.datanode.max.locked.memory must be >= 0", maxLockedMemory >= 0);
    }

    @Test
    public void testMaxLockedMemoryDoesNotExceedOsLimit() throws Exception {
        Configuration conf = new Configuration();
        long maxLockedMemory = conf.getLong("dfs.datanode.max.locked.memory", 0L);

        // Call shell to get ulimit -l (in kB)
        Process proc = Runtime.getRuntime().exec(new String[]{"bash", "-c", "ulimit -l"});
        java.io.InputStream is = proc.getInputStream();
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        String output = s.hasNext() ? s.next().trim() : "0";
        s.close();
        proc.waitFor();

        // 'unlimited' means no limit
        if ("unlimited".equals(output)) return;

        long osLimitKB = Long.parseLong(output);
        long osLimitBytes = osLimitKB * 1024;

        assertTrue(
            "dfs.datanode.max.locked.memory (" + maxLockedMemory +
            ") must not exceed OS RLIMIT_MEMLOCK (" + osLimitBytes + " bytes)",
            maxLockedMemory <= osLimitBytes
        );
    }
}
