package com.xiguzai.shardingsphere.jdbc.algorithm;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class StepShardingAlgorithmTest {

    private Set<String> availableTables;

    @Before
    public void before() {
        availableTables = new HashSet<>();
        for (int i = 1; i <= 10; i++) {
            String table = "test_" + i;
            availableTables.add(table);
        }
    }

    @Test
    public void testDoPreciseSharding() {
        StepShardingAlgorithm alg = new StepShardingAlgorithm(5000000);
        String tableName = alg.doSharding(availableTables, new PreciseShardingValue<>("test", "id", 29999999L));
        assertNotNull(tableName);
        System.out.println(tableName);
    }

    @Test
    public void testRangeDoSharding() {
        StepShardingAlgorithm alg = new StepShardingAlgorithm(5000000);
        Range<Long> range = Range.range(0L, BoundType.CLOSED, 115000000L, BoundType.CLOSED);
        Collection<String> tableNames = alg.doSharding(availableTables, new RangeShardingValue<>("test", "id", range));
        assertTrue(tableNames != null && !tableNames.isEmpty());
        System.out.println(tableNames);
    }
}