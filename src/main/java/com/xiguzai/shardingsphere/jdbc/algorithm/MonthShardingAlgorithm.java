package com.xiguzai.shardingsphere.jdbc.algorithm;

import com.google.common.collect.Range;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class MonthShardingAlgorithm extends AbstractDateShardingAlgorithm implements PreciseShardingAlgorithm<Date>, RangeShardingAlgorithm<Date> {

    private static final Date DEFAULT_MIN_DATE = Date.from(Instant.parse("1970-01-01T00:00:00.00Z").atZone(ZoneId.systemDefault()).toInstant());
    private static final Date DEFAULT_MAX_DATE = Date.from(Instant.parse("2038-01-19T03:14:07.00Z").atZone(ZoneId.systemDefault()).toInstant());

    private Date minDate;
    private Date maxDate;

    private static final int MONTH_STEP = 1;

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Date> shardingValue) {
        return getRouteTableName(availableTargetNames, shardingValue.getLogicTableName(), shardingValue.getColumnName(), shardingValue.getValue());
    }

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, RangeShardingValue<Date> shardingValue) {
        Date lowerDate, upperDate;
        Range<Date> valueRange = shardingValue.getValueRange();
        try {
            lowerDate = valueRange.lowerEndpoint();
        } catch (IllegalStateException e) {
            lowerDate = getMinDate();
        }
        try {
            upperDate = valueRange.upperEndpoint();
        } catch (IllegalStateException e) {
            upperDate = getMaxDate();
        }
        LocalDate lowerLocalDate = LocalDateTime.ofInstant(lowerDate.toInstant(), ZoneId.systemDefault()).toLocalDate();
        LocalDate upperLocalDate = LocalDateTime.ofInstant(upperDate.toInstant(), ZoneId.systemDefault()).toLocalDate();

        Set<String> targetNames = new HashSet<>();
        int diff = upperLocalDate.compareTo(lowerLocalDate);
        if (diff < 0) {
            throw new IllegalArgumentException();
        } else if (diff == 0) {
            addTargetName(targetNames, availableTargetNames, shardingValue.getLogicTableName(), shardingValue.getColumnName(), lowerDate);
        } else {
            LocalDate lowLocalDate = lowerLocalDate;
            while (upperLocalDate.compareTo(lowLocalDate) >= 0) {
                Instant lowInstant = lowLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
                Date lowDate = Date.from(lowInstant);
                addTargetName(targetNames, availableTargetNames, shardingValue.getLogicTableName(), shardingValue.getColumnName(), lowDate);
                lowLocalDate = lowLocalDate.plusMonths(getMonthStep());
            }
            Instant upInstant = upperLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
            Date upDate = Date.from(upInstant);
            addTargetName(targetNames, availableTargetNames, shardingValue.getLogicTableName(), shardingValue.getColumnName(), upDate);
        }

        return targetNames;
    }

    @Override
    protected String getRouteTableNo(int month) {
        return String.valueOf(month);
    }

    public Date getMinDate() {
        return Objects.isNull(minDate) ? DEFAULT_MIN_DATE : minDate;
    }

    public void setMinDate(Date minDate) {
        this.minDate = minDate;
    }

    public Date getMaxDate() {
        return Objects.isNull(maxDate) ? DEFAULT_MAX_DATE : maxDate;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public int getMonthStep() {
        return MONTH_STEP;
    }
}
