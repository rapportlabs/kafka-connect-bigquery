package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.stream.AppendContext;
import com.wepay.kafka.connect.bigquery.write.row.stream.DataWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 아래 문서를 참고하였습니다
 * https://cloud.google.com/bigquery/docs/write-api-streaming
 * https://github.com/googleapis/java-bigquerystorage/blob/HEAD/samples/snippets/src/main/java/com/example/bigquerystorage/WriteToDefaultStream.java
 */
public class StorageWriteBigQueryWriter extends AdaptiveBigQueryWriter {

    private static final Logger logger = LoggerFactory.getLogger(StorageWriteBigQueryWriter.class);

    private final ConcurrentHashMap<PartitionedTableId, DataWriter> writers = new ConcurrentHashMap<>();

    /**
     * @param bigQuery            Used to send write requests to BigQuery.
     * @param schemaManager       Used to update BigQuery tables.
     * @param retry               How many retries to make in the event of a 500/503 error.
     * @param retryWait           How long to wait in between retries.
     * @param autoCreateTables    Whether tables should be automatically created
     * @param errantRecordHandler Used to handle errant records
     */
    public StorageWriteBigQueryWriter(BigQuery bigQuery, SchemaManager schemaManager, int retry, long retryWait, boolean autoCreateTables, ErrantRecordHandler errantRecordHandler) {
        super(bigQuery, schemaManager, retry, retryWait, autoCreateTables, errantRecordHandler);
    }

    @Override
    public Map<Long, List<BigQueryError>> performWriteRequest(PartitionedTableId tableId, SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows) {
        DataWriter writer = writers.computeIfAbsent(tableId, this::createDataWriter);
        try {
            writer.append(envelope(rows));
        } catch (Descriptors.DescriptorValidationException | IOException | InterruptedException e) {
            logger.warn("Failed to append rows to DataWriter", e);
            throw new RuntimeException(e);
        }

        // TODO: 적절한 오류를 담습니다
        return new HashMap<>();
    }

    private DataWriter createDataWriter(PartitionedTableId tableId) {
        DataWriter writer = new DataWriter();
        TableName tableName = TableName.of(tableId.getProject(), tableId.getDataset(), tableId.getBaseTableName());
        try {
            writer.initialize(tableName);
        } catch (Descriptors.DescriptorValidationException | IOException | InterruptedException e) {
            logger.warn("Failed to initialize DataWriter", e);
            throw new RuntimeException(e);
        }
        return writer;
    }

    protected AppendContext envelope(SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows) {
        List<JSONObject> objects = rows.values().stream().map(rowToInsert ->
                new JSONObject(rowToInsert.getContent())
        ).collect(Collectors.toList());
        return new AppendContext(new JSONArray(objects));
    }
}
