package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Category({RegionServerTests.class, SmallTests.class})
public class TestLogRollerIdleNoRoll {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestLogRollerIdleNoRoll.class);

    private Server mockServer;
    private RegionServerServices mockServices;
    private LogRoller logRoller;
    private WAL mockWal;
    private Configuration mockConfiguration;

    @Before
    public void setUp() throws Exception {
        // 1. Create mock objects for Server, RegionServerServices, and Configuration
        mockServer = mock(Server.class);
        mockServices = mock(RegionServerServices.class);
        mockWal = mock(WAL.class);
        mockConfiguration = mock(Configuration.class);

        // 2. Mock configuration retrieval API
        when(mockServer.getConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.getLong("hbase.regionserver.logroll.period", 3600000))
                .thenReturn(60000L); // Use API for roll period
        when(mockConfiguration.getInt("hbase.regionserver.thread.wake.frequency", 10000))
                .thenReturn(10000); // Use API for wake frequency
        when(mockConfiguration.getLong("hbase.regionserver.hlog.check.lowreplication.interval", 30000L))
                .thenReturn(30000L); // Use API for low replication interval

        // 3. Initialize LogRoller with mocked Server and RegionServerServices
        logRoller = new LogRoller(mockServer, mockServices);

        // 4. Prepare state for testing (set lastRollTime and disable WAL roll requests)
        Field lastRollTimeField = LogRoller.class.getDeclaredField("lastRollTime");
        lastRollTimeField.setAccessible(true);
        lastRollTimeField.set(logRoller, System.currentTimeMillis()); // Set recent last roll time

        Field walNeedsRollField = LogRoller.class.getDeclaredField("walNeedsRoll");
        walNeedsRollField.setAccessible(true);
        ConcurrentMap<WAL, Boolean> walNeedsRollMap = new ConcurrentHashMap<>();
        walNeedsRollMap.put(mockWal, Boolean.FALSE); // Set no roll requests
        walNeedsRollField.set(logRoller, walNeedsRollMap);
    }

    @Test
    public void testLogRollerIdleNoRoll() throws Exception {
        // 1. Ensure LogRoller runs for a single iteration (set running to false to terminate loop)
        Field runningField = LogRoller.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(logRoller, false);

        // 2. Start the LogRoller thread and execute the run() method
        Thread testThread = new Thread(() -> {
            try {
                logRoller.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        testThread.start();
        testThread.join();

        // 3. Verify WAL rollWriter is never called
        verify(mockWal, never()).rollWriter(anyBoolean());
    }

    @After
    public void tearDown() throws Exception {
        // 4. Cleanup after testing
        mockServer = null;
        mockServices = null;
        logRoller = null;
        mockWal = null;
        mockConfiguration = null;
    }
}