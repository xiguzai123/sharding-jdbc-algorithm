package com.xiguzai.shardingsphere.jdbc.algorithm;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;

public abstract class AbstractDateShardingAlgorithm extends AbstractShardingAlgorithm<Date> {

    @Override
    protected final String getRouteTableName(Collection<String> availableTargetNames, String logicTableName, String columnName, Date value) {
        StringBuilder sb = new StringBuilder();
        sb.append(logicTableName).append(STRING);

        LocalDate localDate = LocalDateTime.ofInstant(value.toInstant(), ZoneId.systemDefault()).toLocalDate();
        int year = localDate.getYear();
        sb.append(year);
        int month = localDate.getMonthValue();
        String routeTableNo = getRouteTableNo(month);
        if (Objects.nonNull(routeTableNo) && !routeTableNo.isEmpty()) {
            sb.append(STRING).append(routeTableNo);
        }
        String targetName = sb.toString();

        return availableTargetNames.contains(targetName) ? targetName : null;
    }

    protected abstract String getRouteTableNo(int month);
}
