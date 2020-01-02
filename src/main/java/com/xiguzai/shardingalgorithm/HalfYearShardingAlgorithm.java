package com.xiguzai.shardingalgorithm;

import java.util.Optional;

public class HalfYearShardingAlgorithm extends MonthShardingAlgorithm {

    private static final int MONTH_STEP = 6;

    @Override
    protected Optional<String> getRouteTableNo(int month) {
        int i = (month + MONTH_STEP - 1) / MONTH_STEP;
        if (i == 0) {
            i = 1;
        }
        return Optional.of(String.valueOf(i));
    }

    @Override
    public int getMonthStep() {
        return MONTH_STEP;
    }
}
