package com.wepay.kafka.connect.bigquery.write.row.stream;

import org.json.JSONArray;

public class AppendContext {

    JSONArray data;

    public AppendContext(JSONArray data) {
        this.data = data;
    }
}
