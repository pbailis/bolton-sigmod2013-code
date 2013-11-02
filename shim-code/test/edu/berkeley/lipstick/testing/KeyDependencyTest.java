package edu.berkeley.lipstick.testing;

import edu.berkeley.lipstick.util.KeyDependency;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyDependencyTest {
    /*
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidWriteNumber() throws Exception {
        new KeyDependency("testWriter", 0, 0);
    }

    @Test
    public void testMatch() throws Exception {
        assertTrue((new KeyDependency("testWriter", 2, 2)).match((new KeyDependency("testWriter", 2, 2))));
    }

    @Test
    public void testWillOverwrite() throws Exception {
        assertTrue((new KeyDependency("testWriter", 2, 3)).willOverwrite((new KeyDependency("testWriter", 2, 2))));
        assertFalse((new KeyDependency("testWriter", 2, 1)).willOverwrite((new KeyDependency("testWriter", 2, 2))));
        assertTrue((new KeyDependency("testWriter2", 2, 3)).willOverwrite((new KeyDependency("testWriter1", 2, 3))));
    }
    */
}