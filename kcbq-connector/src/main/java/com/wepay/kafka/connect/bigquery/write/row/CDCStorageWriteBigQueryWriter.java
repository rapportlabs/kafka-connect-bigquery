package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.write.row.stream.AppendContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 * 아래 문서를 참고하였습니다
 * https://cloud.google.com/bigquery/docs/change-data-capture
 */
public class CDCStorageWriteBigQueryWriter extends StorageWriteBigQueryWriter {

    // https://cloud.google.com/bigquery/docs/change-data-capture#specify_changes_to_existing_records
    private static final String BIGQUERY_CDC_CHANGE_TYPE = "_CHANGE_TYPE";
    private static final String BIGQUERY_CDC_UPSERT_VALUE = "UPSERT";
    private static final String BIGQUERY_CDC_DELETE_VALUE = "DELETE";

    // TODO: https://cloud.google.com/bigquery/docs/change-data-capture#format 를 활용하여 순서보장을 진행할 수 있습니다
    private static final String BIGQUERY_CDC_CHANGE_SEQUENCE_NUMBER = "_CHANGE_SEQUENCE_NUMBER";

    private final CDCOptions cdcOptions;

    /**
     * @param bigQuery            Used to send write requests to BigQuery.
     * @param schemaManager       Used to update BigQuery tables.
     * @param retry               How many retries to make in the event of a 500/503 error.
     * @param retryWait           How long to wait in between retries.
     * @param autoCreateTables    Whether tables should be automatically created
     * @param errantRecordHandler Used to handle errant records
     */
    public CDCStorageWriteBigQueryWriter(BigQuery bigQuery,
                                         SchemaManager schemaManager,
                                         int retry,
                                         long retryWait,
                                         boolean autoCreateTables,
                                         ErrantRecordHandler errantRecordHandler,
                                         CDCOptions cdcOptions) {
        super(bigQuery, schemaManager, retry, retryWait, autoCreateTables, errantRecordHandler);
        this.cdcOptions = cdcOptions;
    }

    @Override
    protected AppendContext envelope(SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows) {
        List<JSONObject> objects = rows.values().stream().map(rowToInsert ->
                new JSONObject(filterCdcData(rowToInsert.getContent()))
        ).collect(Collectors.toList());
        return new AppendContext(new JSONArray(objects));
    }

    // TODO: https://cloud.google.com/bigquery/docs/change-data-capture#format 를 활용하여 순서보장을 진행할 수 있습니다
    private Map<String, Object> filterCdcData(Map<String, Object> map) {
        Map<String, Object> filteredMap = new HashMap<>();
        map.forEach((key, value) -> {
            if (key.startsWith(cdcOptions.cdcPrefixAfter)) {
                filteredMap.put(key.substring(cdcOptions.cdcPrefixAfter.length()), value);
            } else if (key.equals(cdcOptions.cdcChangeType)) {
                if (value instanceof String && cdcOptions.cdcUpsertTypes.contains(value)) {
                    filteredMap.put(BIGQUERY_CDC_CHANGE_TYPE, BIGQUERY_CDC_UPSERT_VALUE);
                } else if (value instanceof String && cdcOptions.cdcDeleteTypes.contains(value)) {
                    filteredMap.put(BIGQUERY_CDC_CHANGE_TYPE, BIGQUERY_CDC_DELETE_VALUE);
                }
            }
        });
        return filteredMap;
    }
}
