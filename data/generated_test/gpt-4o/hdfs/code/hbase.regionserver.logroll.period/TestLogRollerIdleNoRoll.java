package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.*;

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
        // 1. Create mock objects for Server, RegionServerServices, and Configuration.
        mockServer = mock(Server.class);
        mockServices = mock(RegionServerServices.class);
        mockWal = mock(WAL.class);
        mockConfiguration = mock(Configuration.class);

        // 2. Mock configuration retrieval API to fetch the rollPeriod value.
        when(mockServer.getConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.getLong("hbase.regionserver.logroll.period", 3600000))
                .thenReturn(60000L);
        when(mockConfiguration.getInt("hbase.regionserver.thread.wake.frequency", 10000))
                .thenReturn(10000);
        when(mockConfiguration.getLong("hbase.regionserver.hlog.check.lowreplication.interval", 30000L))
                .thenReturn(30000L);

        // 3. Initialize LogRoller with mocked Server and RegionServerServices.
        logRoller = new LogRoller(mockServer, mockServices);

        // 4. Prepare state: set lastRollTime to the current time and no roll requests using reflection.
        Field lastRollTimeField = LogRoller.class.getDeclaredField("lastRollTime");
        lastRollTimeField.setAccessible(true);
        lastRollTimeField.set(logRoller, System.currentTimeMillis());
        
        Field walNeedsRollField = LogRoller.class.getDeclaredField("walNeedsRoll");
        walNeedsRollField.setAccessible(true);
        ConcurrentMap<WAL, Boolean> walNeedsRollMap = new ConcurrentHashMap<>();
        walNeedsRollMap.put(mockWal, Boolean.FALSE);
        walNeedsRollField.set(logRoller, walNeedsRollMap);
    }

    @Test
    public void testLogRollerIdleNoRoll() throws Exception {
        // 1. Use reflection to set the running field to false for this test case.
        Field runningField = LogRoller.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(logRoller, false); // Terminate after a single iteration.

        // 2. Call the run() method directly in an idle state within a test thread.
        Thread testThread = new Thread(() -> {
            try {
                logRoller.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        testThread.start();
        testThread.join();

        // 3. Verify WAL rollWriter method is never called.
        verify(mockWal, never()).rollWriter(anyBoolean());
    }

    @After
    public void tearDown() throws Exception {
        // Cleanup: reset mock instances and release resources.
        mockServer = null;
        mockServices = null;
        logRoller = null;
        mockWal = null;
        mockConfiguration = null;
    }
}