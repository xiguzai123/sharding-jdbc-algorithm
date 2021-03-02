package com.xiguzai.shardingsphere.jdbc.algorithm;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class MonthShardingAlgorithmTest {

    private Set<String> availableTables;

    @Before
    public void before() {
        int year = 2021;
        availableTables = new HashSet<>();
        for (int i = 1; i <= 12; i++) {
            String table = "test_" + year + "_" + i;
            availableTables.add(table);
        }
    }

    @Test
    public void testDoPreciseSharding() {
        MonthShardingAlgorithm alg = new MonthShardingAlgorithm();
        String tableName = alg.doSharding(availableTables, new PreciseShardingValue<>("test", "date", new Date()));
        assertNotNull(tableName);
        System.out.println(tableName);
    }

    @Test
    public void testRangeDoSharding() throws ParseException {
        MonthShardingAlgorithm alg = new MonthShardingAlgorithm();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date low = format.parse("2021-01-01");
        Date up = format.parse("2021-12-31");
        Range<Date> range = Range.range(low, BoundType.OPEN, up, BoundType.OPEN);
        Collection<String> tables = alg.doSharding(availableTables, new RangeShardingValue<>("test", "date", range));
        assertTrue(tables != null && !tables.isEmpty());
        System.out.println(tables);
    }
}