package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.stream.AppendContext;
import com.wepay.kafka.connect.bigquery.write.row.stream.DataWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

public class StorageWriteBigQueryWriter extends UpsertDeleteBigQueryWriter {

    /**
     * @param bigQuery                        Used to send write requests to BigQuery.
     * @param schemaManager                   Used to update BigQuery tables.
     * @param retry                           How many retries to make in the event of a 500/503 error.
     * @param retryWait                       How long to wait in between retries.
     * @param autoCreateTables                Whether destination tables should be automatically created
     * @param intermediateToDestinationTables A mapping used to determine the destination table for
     *                                        given intermediate tables; used for create/update
     *                                        operations in order to propagate them to the destination
     *                                        table
     * @param errantRecordHandler             Used to handle errant records
     */
    public StorageWriteBigQueryWriter(BigQuery bigQuery, SchemaManager schemaManager, int retry, long retryWait, boolean autoCreateTables, Map<TableId, TableId> intermediateToDestinationTables, ErrantRecordHandler errantRecordHandler) {
        super(bigQuery, schemaManager, retry, retryWait, autoCreateTables, intermediateToDestinationTables, errantRecordHandler);
    }

    @Override
    public Map<Long, List<BigQueryError>> performWriteRequest(PartitionedTableId tableId, SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows) {
        DataWriter writer = new DataWriter();
        TableName tableName = TableName.of(tableId.getProject(), tableId.getDataset(), tableId.getBaseTableName());
        try {
            writer.initialize(tableName);
            List<JSONObject> objects = rows.values().stream().map(rowToInsert ->
                    new JSONObject(rowToInsert.getContent())
            ).collect(Collectors.toList());
            writer.append(new AppendContext(new JSONArray(objects)));

        } catch (Descriptors.DescriptorValidationException e) {
            // TODO: 오류를 적절하게 처리합니다
            throw new RuntimeException(e);
        } catch (IOException e) {
            // TODO: 오류를 적절하게 처리합니다
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            // TODO: 오류를 적절하게 처리합니다
            throw new RuntimeException(e);
        }

        // TODO: 적절한 오류를 담습니다
        return new HashMap<>();
    }
}
