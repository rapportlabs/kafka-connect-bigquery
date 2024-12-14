package com.wepay.kafka.connect.bigquery.write.row;

import java.util.Collection;

public class CDCOptions {
    String cdcPrefixBefore;
    String cdcPrefixAfter;
    String cdcChangeType;
    Collection<String> cdcUpsertTypes;
    Collection<String> cdcDeleteTypes;

    public CDCOptions(
            String cdcPrefixBefore,
            String cdcPrefixAfter,
            String cdcChangeType,
            Collection<String> cdcUpsertTypes,
            Collection<String> cdcDeleteTypes
            ) {
        this.cdcPrefixBefore = cdcPrefixBefore;
        this.cdcPrefixAfter = cdcPrefixAfter;
        this.cdcChangeType = cdcChangeType;
        this.cdcUpsertTypes = cdcUpsertTypes;
        this.cdcDeleteTypes = cdcDeleteTypes;
    }
}
