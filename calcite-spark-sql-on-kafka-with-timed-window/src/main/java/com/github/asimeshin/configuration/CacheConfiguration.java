package com.github.asimeshin.configuration;

import com.github.asimeshin.repository.MemoryCache;
import com.github.asimeshin.service.SparkSqlService;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.TimeUnit;

public class CacheConfiguration {

    @Bean
    public Cache<String, SparkSqlService.TransferEntity> transfersCache() {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .weakKeys()
                .weakValues()
                .recordStats()
                .build();
    }

    @Bean
    public Cache<String, SparkSqlService.FraudEvent> fraudEventsCache() {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .weakKeys()
                .weakValues()
                .recordStats()
                .build();
    }

    @Bean
    MemoryCache guavaCache() {
    }
}
