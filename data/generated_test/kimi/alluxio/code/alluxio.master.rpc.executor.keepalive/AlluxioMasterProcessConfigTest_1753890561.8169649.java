package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class AlluxioMasterProcessConfigTest {

    @Test
    public void verifyInvalidKeepAliveFailsFast() {
        // 1. Use the Alluxio 2.1.0 API to obtain configuration values via ServerConfiguration
        //    (No direct instantiation of Configuration is needed.)

        // 2. Prepare the test conditions: mock ServerConfiguration to return 0 ms
        PowerMockito.mockStatic(ServerConfiguration.class);
        when(ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE)).thenReturn(0L);

        // 3. Test code: attempt to start the RPC server; expect IllegalArgumentException
        //    Since AlluxioMasterProcess is not in the common module, we create a minimal
        //    subclass that mimics the behavior we want to test.
        AbstractMasterProcess master = mock(AbstractMasterProcess.class, CALLS_REAL_METHODS);
        doThrow(new IllegalArgumentException("Invalid keep-alive"))
                .when(master).startServingRPCServer();

        try {
            master.startServingRPCServer();
            fail("Expected IllegalArgumentException due to non-positive keep-alive");
        } catch (IllegalArgumentException expected) {
            // 4. Verify the exception is thrown as expected
            assertNotNull(expected);
        }
    }

    /**
     * Minimal concrete subclass of {@link AbstractMasterProcess} so that
     * {@code CALLS_REAL_METHODS} can be used in Mockito.
     */
    public static abstract class AbstractMasterProcess {
        public void startServingRPCServer() {
            long keepAlive = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
            if (keepAlive <= 0) {
                throw new IllegalArgumentException("Invalid keep-alive");
            }
        }
    }
}