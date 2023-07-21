# sharding-jdbc-algorithm

## 使用说明
该库所有时间分片算法实现基于`4.x.x`版本`sharding-jdbc`，当时官方没有实现时间分片算法. 现在官方最新`5.x.x`版本已支持时间分片算法，[点击查看官方文档](https://shardingsphere.apache.org/document/current/cn/features/sharding/concept/#自动化分片算法).

## sharding-jdbc分片算法实现合集

- 按年分表算法 -> YearShardingAlgorithm
- 半年分表算法 -> HalfYearShardingAlgorithm
- 季度分表算法 -> SeasonShardingAlgorithm
- 按月分表算法 -> MonthShardingAlgorithm
- 有序自增id分段分片算法 -> StepShardingAlgorithm
