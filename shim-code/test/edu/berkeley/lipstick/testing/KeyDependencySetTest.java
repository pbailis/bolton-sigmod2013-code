package edu.berkeley.lipstick.testing;

import edu.berkeley.lipstick.util.KeyDependency;
import edu.berkeley.lipstick.util.KeyDependencySet;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyDependencySetTest {
    /*
    @Test
    public void testPutGetDependency() throws Exception {
        KeyDependencySet kds = new KeyDependencySet();

        kds.putDependency("foo", new KeyDependency("testWriter", 1, 2));
        assertEquals("testWriter", kds.getDependency("foo").getWriter());
        assertEquals(1, kds.getDependency("foo").getWriteNumber());
        assertEquals(2, kds.getDependency("foo").getTimestamp());
        assertEquals(1, kds.getKeys().size());
        assertTrue(kds.getKeys().contains("foo"));

        kds.putDependency("foo", new KeyDependency("testWriter", 3, 4));
        assertEquals("testWriter", kds.getDependency("foo").getWriter(), "testWriter");
        assertEquals(3, kds.getDependency("foo").getWriteNumber());
        assertEquals(4, kds.getDependency("foo").getTimestamp());
        assertEquals(1, kds.getKeys().size());
        assertTrue(kds.getKeys().contains("foo"));

        kds.putDependency("bar", new KeyDependency("testWriter2", 5, 6));
        assertEquals("testWriter2", kds.getDependency("bar").getWriter());
        assertEquals(5, kds.getDependency("bar").getWriteNumber());
        assertEquals(6, kds.getDependency("bar").getTimestamp());
        assertEquals(2, kds.getKeys().size());
        assertTrue(kds.getKeys().contains("bar"));
    }

    @Test
    public void testIntersectSet() throws Exception {
        KeyDependencySet a = new KeyDependencySet();
        a.putDependency("x", new KeyDependency("foo", 1, 0));
        a.putDependency("y", new KeyDependency("foo", 2, 4));

        KeyDependencySet b = new KeyDependencySet();
        b.putDependency("x", new KeyDependency("foo", 1, 0));
        b.putDependency("y", new KeyDependency("bar", 3, 8));

        KeyDependencySet c = new KeyDependencySet();
        c.putDependency("x", new KeyDependency("foo", 1, 0));
        c.putDependency("y", new KeyDependency("foo", 3, 8));

        a.intersectSet(b);
        assertEquals(4, a.getDependency("y").getTimestamp());
    }

    @Test
    public void testMergeSet() throws Exception {
        KeyDependencySet kds1 = new KeyDependencySet();
        KeyDependencySet kds2 = new KeyDependencySet();

        kds1.putDependency("foo", new KeyDependency("testWriter", 1, 1));
        kds2.putDependency("bar", new KeyDependency("testWriter", 1, 2));

        kds1.mergeSet(kds2);

        assertEquals(1, kds2.getKeys().size());
        assertTrue(kds2.getDependency("bar").match(new KeyDependency("testWriter", 1, 2)));

        assertEquals(2, kds1.getKeys().size());
        assertTrue(kds1.getDependency("foo").match(new KeyDependency("testWriter", 1, 1)));
        assertTrue(kds1.getDependency("bar").match(new KeyDependency("testWriter", 1, 2)));

        //merging with a lower timestamp shouldn't affect kds1
        kds2.putDependency("foo", new KeyDependency("testWriter", 4, 0));
        kds1.mergeSet(kds2);

        assertEquals(2, kds1.getKeys().size());
        assertTrue(kds1.getDependency("foo").match(new KeyDependency("testWriter", 1, 1)));
        assertTrue(kds1.getDependency("bar").match(new KeyDependency("testWriter", 1, 2)));

        //merging with a higher timestamp should affect kds1
        kds2.putDependency("foo", new KeyDependency("testWriter2", 4, 100));
        kds1.mergeSet(kds2);

        assertEquals(2, kds1.getKeys().size());
        assertTrue(kds1.getDependency("foo").match(new KeyDependency("testWriter2", 4, 100)));
        assertTrue(kds1.getDependency("bar").match(new KeyDependency("testWriter", 1, 2)));
    }
    */
}