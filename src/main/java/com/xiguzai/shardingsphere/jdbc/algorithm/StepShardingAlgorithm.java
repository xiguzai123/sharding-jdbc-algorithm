package com.xiguzai.shardingsphere.jdbc.algorithm;

import com.google.common.collect.Range;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class StepShardingAlgorithm extends AbstractShardingAlgorithm<Long> implements PreciseShardingAlgorithm<Long>, RangeShardingAlgorithm<Long> {

    public StepShardingAlgorithm(long step) {
        this.step = step;
    }

    public StepShardingAlgorithm(long step, Long min, Long max) {
        this.min = min;
        this.max = max;
        this.step = step;
    }

    private static final long defaultMin = 0;
    private static final long defaultMax = Integer.MAX_VALUE;

    private Long min;
    private Long max;

    private long step;

    @Override
    protected String getRouteTableName(Collection<String> availableTargetNames, String logicTableName, String columnName, Long value) {
        StringBuilder sb = new StringBuilder();
        sb.append(logicTableName).append(STRING);
        String suffix = suffix(value);
        sb.append(suffix);
        String routeTableName = sb.toString();
        return availableTargetNames.contains(routeTableName) ? routeTableName : null;
    }

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Long> shardingValue) {
        return getRouteTableName(availableTargetNames, shardingValue.getLogicTableName(), shardingValue.getColumnName(), shardingValue.getValue());
    }

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, RangeShardingValue<Long> shardingValue) {
        long lower, upper;
        String logicTableName = shardingValue.getLogicTableName();
        String columnName = shardingValue.getColumnName();
        Range<Long> valueRange = shardingValue.getValueRange();
        try {
            lower = valueRange.lowerEndpoint();
        } catch (IllegalStateException e) {
            lower = getMin();
        }
        try {
            upper = valueRange.upperEndpoint();
        } catch (IllegalStateException e) {
            upper = getMax();
        }
        Set<String> targetNames = new HashSet<>();
        long diff = upper - lower;
        if (diff < 0) {
            throw new IllegalArgumentException();
        } else if (diff == 0) {
            addTargetNames(targetNames, availableTargetNames, logicTableName, columnName, lower);
        } else {
            long low = lower;
            while (upper - low >= 0) {
                addTargetNames(targetNames, availableTargetNames, logicTableName, columnName, low);
                low += step;
            }
            addTargetNames(targetNames, availableTargetNames, logicTableName, columnName, upper);
        }
        return targetNames;
    }

    private String suffix(long value) {
        long i = value / (step + 1) + 1;
        return String.valueOf(i);
    }

    public Long getMin() {
        return Objects.isNull(min) ? defaultMin : min;
    }

    public void setMin(Long min) {
        this.min = min;
    }

    public Long getMax() {
        return Objects.isNull(max) ? defaultMax : max;
    }

    public void setMax(Long max) {
        this.max = max;
    }
}
