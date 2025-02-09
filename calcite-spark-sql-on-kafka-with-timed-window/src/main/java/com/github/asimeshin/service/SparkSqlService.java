package com.github.asimeshin.service;

import com.github.asimeshin.controller.KafkaEventStreamListener;
import com.github.asimeshin.repository.MemoryCache;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static com.github.asimeshin.service.SparkSqlUtils.setSchema;

@AllArgsConstructor
public class SparkSqlService {

    private MemoryCache memoryCache;
    private DataSource dataSource;

    @SneakyThrows
    public Optional<FraudEvent> checkFraud(String id, KafkaEventStreamListener.Event incomingEvent) {
        final TransferEntity transferEntity = new TransferEntity();
        transferEntity.setId(id);
        transferEntity.setFrom_user_id(incomingEvent.userFrom());
        transferEntity.setTo_user_id(incomingEvent.userTo());
        transferEntity.setPayment(incomingEvent.payment());
        transferEntity.setCurrency(incomingEvent.currency());
        memoryCache.getTransfersCache().put(id, transferEntity);

        final SparkSqlSchema temporalSnapshotSchema = new SparkSqlSchema();
        temporalSnapshotSchema.setTransfers(memoryCache.getTransfers());
        temporalSnapshotSchema.setFraud_evens(memoryCache.getFraudEvents());

        try (Connection connection = dataSource.getConnection()) {
            final CalciteConnection unwrap = connection.unwrap(CalciteConnection.class);
            setSchema(unwrap, "example_schema", temporalSnapshotSchema);

            final PreparedStatement statement = unwrap.prepareStatement("""
                SELECT
                    COUNT(*) AS transfer_count
                FROM
                    transfers t
                LEFT JOIN
                    fraud_events f ON t.from_user_id = f.user_id
                    AND f.registration_date_time >= TIMESTAMPADD(MINUTE, -5, CURRENT_TIMESTAMP)
                WHERE
                    t.timestamp >= TIMESTAMPADD(MINUTE, -5, CURRENT_TIMESTAMP)
                    AND t.from_user_id <> t.to_user_id
                    AND t.payment > 10000
                    AND f.user_id IS NULL
                    AND t.from_user_id = ?
                GROUP BY
                    t.from_user_id
                """
            );
            statement.setObject(1, incomingEvent.userFrom());

            final ResultSet resultSet = statement.getResultSet();
            final int counter = resultSet.getInt(0);
            if (counter < 10) return Optional.empty();

            final FraudEvent newFraudEvent = new FraudEvent();
            newFraudEvent.setId(UUID.randomUUID().toString());
            newFraudEvent.setUser_id(incomingEvent.userFrom());
            newFraudEvent.setRegistration_date_time(LocalDateTime.now().toString());

            memoryCache.getFraudEventsCache().put(newFraudEvent.getId(), newFraudEvent);

            return Optional.of(newFraudEvent);
        }
    }

    @Data
    public static class SparkSqlSchema {
        public TransferEntity[] transfers;
        public FraudEvent[] fraud_evens;
    }

    @Data
    public static class TransferEntity {
        public String id;
        public String from_user_id;
        public String to_user_id;
        public Double payment;
        public String currency;
    }

    @Data
    public static class FraudEvent {
        public String id;
        public String registration_date_time;
        public String user_id;
    }
}
