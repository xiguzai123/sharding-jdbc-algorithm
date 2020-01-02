package com.xiguzai.shardingalgorithm;

import java.util.Optional;

public class YearShardingAlgorithm extends MonthShardingAlgorithm {
    private static final int MONTH_STEP = 12;

    @Override
    protected Optional<String> getRouteTableNo(int month) {
        return Optional.empty();
    }

    @Override
    public int getMonthStep() {
        return MONTH_STEP;
    }
}
