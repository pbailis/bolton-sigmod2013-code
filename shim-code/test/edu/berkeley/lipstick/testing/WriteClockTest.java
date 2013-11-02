package edu.berkeley.lipstick.testing;

import edu.berkeley.lipstick.util.WriteClock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WriteClockTest {
    @Test
    public void testGetValue() throws Exception {
        WriteClock wc = new WriteClock();
        assertEquals(0, wc.getValue("test"));
        wc.incrementValue("test");
        assertEquals(1, wc.getValue("test"));
    }

    @Test
    public void testGetWriters() throws Exception {
        WriteClock wc = new WriteClock();
        assertEquals(0, wc.getWriters().size());
        wc.incrementValue("test");
        assertEquals(1, wc.getWriters().size());
        assertTrue(wc.getWriters().contains("test"));
    }

    @Test
    public void testMergeClock() throws Exception {
        WriteClock wc1 = new WriteClock();
        WriteClock wc2 = new WriteClock();

        //<A:2, B:1>
        wc1.incrementValue("A");
        wc1.incrementValue("A");
        wc1.incrementValue("B");

        //<A:3, C:1>
        wc2.incrementValue("A");
        wc2.incrementValue("A");
        wc2.incrementValue("A");
        wc2.incrementValue("C");

        wc1.mergeClock(wc2);

        //shouldn't touch wc2
        assertEquals(3, wc2.getValue("A"));
        assertEquals(1, wc2.getValue("C"));
        assertEquals(2, wc2.getWriters().size());

        //Should have <A:3, B:1, C:1>

        assertEquals(3, wc1.getValue("A"));
        assertEquals(1, wc1.getValue("B"));
        assertEquals(1, wc1.getValue("C"));
        assertEquals(3, wc1.getWriters().size());
    }

    @Test
    public void testHappensBefore() throws Exception {
        WriteClock wc1 = new WriteClock();
        WriteClock wc2 = new WriteClock();
        WriteClock wc3 = new WriteClock();

        //<A:2, B:1>
        wc1.incrementValue("A");
        wc1.incrementValue("A");
        wc1.incrementValue("B");

        //<A:3, C:1>
        wc2.incrementValue("A");
        wc2.incrementValue("A");
        wc2.incrementValue("A");
        wc2.incrementValue("C");

        //<A:1, B:1>
        wc3.incrementValue("A");
        wc3.incrementValue("B");

        assertFalse(wc1.happensBefore(wc2));
        assertFalse(wc2.happensBefore(wc1));
        assertTrue(wc3.happensBefore(wc1));
        assertFalse(wc2.happensBefore(wc3));
        assertFalse(wc3.happensBefore(wc3));
    }
}