package me.ericfu.lynx.source.jdbc;

import com.google.common.collect.Range;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JdbcSourceTest {

    @Test
    public void buildSplitRanges() {
        List<Range<Long>> ranges = JdbcSource.buildSplitRanges(0, 10, 3);
        assertEquals(Arrays.asList(
            Range.atMost(3L),
            Range.openClosed(3L, 6L),
            Range.greaterThan(6L)
        ), ranges);
    }
}