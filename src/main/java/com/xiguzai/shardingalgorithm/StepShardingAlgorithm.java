package com.xiguzai.shardingalgorithm;

import com.google.common.collect.Range;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;

import java.util.*;

public class StepShardingAlgorithm extends AbstractShardingAlgorithm<Integer> implements PreciseShardingAlgorithm<Integer>, RangeShardingAlgorithm<Integer> {

    public StepShardingAlgorithm(int step) {
        this.step = step;
    }

    public StepShardingAlgorithm(int step, Integer min, Integer max) {
        this.min = min;
        this.max = max;
        this.step = step;
    }

    private static final int defaultMin = 0;
    private static final int defaultMax = Integer.MAX_VALUE;

    private Integer min;
    private Integer max;

    private int step;

    @Override
    Optional<String> getRouteTableName(Collection<String> availableTargetNames, String logicTableName, String columnName, Integer value) {
        StringBuilder sb = new StringBuilder();
        sb.append(logicTableName).append(SPLIT);
        String suffix = suffix(value);
        sb.append(suffix);
        return Optional.of(sb.toString());
    }

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Integer> shardingValue) {
        Optional<String> routeTableName = getRouteTableName(availableTargetNames, shardingValue.getLogicTableName(), shardingValue.getColumnName(), shardingValue.getValue());
        return routeTableName.get();
    }

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, RangeShardingValue<Integer> shardingValue) {
        int lower, upper;
        String logicTableName = shardingValue.getLogicTableName();
        String columnName = shardingValue.getColumnName();
        Range<Integer> valueRange = shardingValue.getValueRange();
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
        int diff = upper - lower;
        if (diff < 0) {
            throw new IllegalArgumentException();
        } else if (diff == 0) {
            addTargetNames(targetNames, availableTargetNames, logicTableName, columnName, lower);
        } else {
            int low = lower;
            while (upper - low >= 0) {
                addTargetNames(targetNames, availableTargetNames, logicTableName, columnName, low);
                low += step;
            }
            addTargetNames(targetNames, availableTargetNames, logicTableName, columnName, upper);
        }
        return targetNames;
    }

    private String suffix(int value) {
        int i = (value + step - 1) / step;
        if (i == 0) {
            i = 1;
        }
        return String.valueOf(i);
    }

    public Integer getMin() {
        return Objects.isNull(min) ? defaultMin : min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }

    public Integer getMax() {
        return Objects.isNull(max) ? defaultMax : max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }
}
