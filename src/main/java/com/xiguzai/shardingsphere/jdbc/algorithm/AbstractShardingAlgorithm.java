package com.xiguzai.shardingsphere.jdbc.algorithm;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * AbstractShardingAlgorithm
 *
 * @author xiguzai
 */
public abstract class AbstractShardingAlgorithm<T extends Comparable<?>> {

    protected static final String STRING = "_";

    /**
     * getRouteTableName
     *
     * @param availableTargetNames
     * @param logicTableName
     * @param columnName
     * @param value
     * @return
     */
    protected abstract String getRouteTableName(Collection<String> availableTargetNames, String logicTableName, String columnName, T value);

    protected String addTargetName(Set<String> targetNames, Collection<String> availableTargetNames, String logicTableName, String columnName, T value) {
        String routeTableName = getRouteTableName(availableTargetNames, logicTableName, columnName, value);
        if (Objects.nonNull(routeTableName) && !routeTableName.isEmpty()) {
            targetNames.add(routeTableName);
        }
        return routeTableName;
    }
}
