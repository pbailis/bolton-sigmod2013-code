package edu.berkeley.lipstick.testing;

import edu.berkeley.lipstick.util.WriteRecorder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WriteRecorderTest {
    @Test
    public void testGetWriteNumber() throws Exception {
        WriteRecorder wr = new WriteRecorder();
        assertEquals(1, wr.getWriteNumber("foo"));
        assertEquals(1, wr.getWriteNumber("bar"));
        assertEquals(2, wr.getWriteNumber("foo"));
        assertEquals(3, wr.getWriteNumber("foo"));
        assertEquals(2, wr.getWriteNumber("bar"));
        assertEquals(4, wr.getWriteNumber("foo"));
    }
}