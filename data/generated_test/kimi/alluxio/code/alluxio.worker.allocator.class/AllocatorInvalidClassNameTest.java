package alluxio.worker.block.allocator;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMetadataView;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AllocatorInvalidClassNameTest {

    @Test
    public void testInvalidClassNameThrowsRuntimeException() {
        // 1. Fresh ServerConfiguration instance
        ServerConfiguration.reset();

        // 2. Mock BlockMetadataView
        BlockMetadataView mockView = mock(BlockMetadataView.class);

        // 3. Set invalid class name
        ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, "non.existing.Allocator");

        // 4. Attempt instantiation and expect RuntimeException
        try {
            Allocator.Factory.create(mockView);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            // 5. Verify wrapped exception is ClassNotFoundException
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
    }
}