package com.xiguzai.shardingsphere.jdbc.algorithm;

public class YearShardingAlgorithm extends MonthShardingAlgorithm {
    private static final int MONTH_STEP = 12;

    @Override
    public int getMonthStep() {
        return MONTH_STEP;
    }
}
