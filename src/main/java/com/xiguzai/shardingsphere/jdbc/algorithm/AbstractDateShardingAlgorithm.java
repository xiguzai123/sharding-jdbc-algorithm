package com.xiguzai.shardingsphere.jdbc.algorithm;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;

public abstract class AbstractDateShardingAlgorithm extends AbstractShardingAlgorithm<Date> {

    @Override
    protected final Optional<String> getRouteTableName(Collection<String> availableTargetNames, String logicTableName, String columnName, Date value) {
        StringBuilder sb = new StringBuilder();
        sb.append(logicTableName).append(split);

        LocalDate localDate = LocalDateTime.ofInstant(value.toInstant(), ZoneId.systemDefault()).toLocalDate();
        int year = localDate.getYear();
        sb.append(year);
        int month = localDate.getMonthValue();
        Optional<String> routeTableNo = getRouteTableNo(month);
        routeTableNo.ifPresent(s -> sb.append(split).append(s));
        String targetName = sb.toString();

        return availableTargetNames.contains(targetName) ? Optional.of(targetName) : Optional.empty();
    }

    protected abstract Optional<String> getRouteTableNo(int month);
}
