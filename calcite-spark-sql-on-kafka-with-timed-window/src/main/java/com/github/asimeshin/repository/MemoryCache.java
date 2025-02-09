package com.github.asimeshin.repository;

import com.github.asimeshin.service.SparkSqlService;
import com.google.common.cache.Cache;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@SuppressWarnings("DataFlowIssue")
public class MemoryCache {
    private Cache<String, SparkSqlService.TransferEntity> transfersCache;
    private Cache<String, SparkSqlService.FraudEvent> fraudEventsCache;

    public SparkSqlService.TransferEntity[] getTransfers() {
        return (SparkSqlService.TransferEntity[]) transfersCache.asMap().values().toArray();
    }

    public SparkSqlService.FraudEvent[] getFraudEvents() {
        return (SparkSqlService.FraudEvent[]) fraudEventsCache.asMap().values().toArray();
    }
}
