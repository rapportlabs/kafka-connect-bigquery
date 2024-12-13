package com.wepay.kafka.connect.bigquery.write.row.stream;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

public class DataWriter {

    private static final int MAX_RECREATE_COUNT = 3;

    private BigQueryWriteClient client;

    // Track the number of in-flight requests to wait for all responses before shutting down.
    private final Phaser inflightRequestCount = new Phaser(1);
    private final Object lock = new Object();
    private JsonStreamWriter streamWriter;

    @GuardedBy("lock")
    private RuntimeException error = null;

    private AtomicInteger recreateCount = new AtomicInteger(0);

    private JsonStreamWriter createStreamWriter(String tableName)
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Configure in-stream automatic retry settings.
        // Error codes that are immediately retried:
        // * ABORTED, UNAVAILABLE, CANCELLED, INTERNAL, DEADLINE_EXCEEDED
        // Error codes that are retried with exponential backoff:
        // * RESOURCE_EXHAUSTED
        RetrySettings retrySettings =
                RetrySettings.newBuilder()
                        .setInitialRetryDelay(Duration.ofMillis(500))
                        .setRetryDelayMultiplier(1.1)
                        .setMaxAttempts(5)
                        .setMaxRetryDelay(Duration.ofMinutes(1))
                        .build();

        // Use the JSON stream writer to send records in JSON format. Specify the table name to write
        // to the default stream.
        // For more information about JsonStreamWriter, see:
        // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
        return JsonStreamWriter.newBuilder(tableName, client)
                .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
                .setChannelProvider(
                        BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                                .setKeepAliveTime(org.threeten.bp.Duration.ofMinutes(1))
                                .setKeepAliveTimeout(org.threeten.bp.Duration.ofMinutes(1))
                                .setKeepAliveWithoutCalls(true)
                                .setChannelsPerCpu(2)
                                .build())
                .setEnableConnectionPool(true)
                // If value is missing in json and there is a default value configured on bigquery
                // column, apply the default value to the missing value field.
                .setDefaultMissingValueInterpretation(
                        AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)
                .setRetrySettings(retrySettings)
                .build();
    }

    public void initialize(TableName parentTable)
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Initialize client without settings, internally within stream writer a new client will be
        // created with full settings.
        client = BigQueryWriteClient.create();

        streamWriter = createStreamWriter(parentTable.toString());
    }

    public void append(AppendContext appendContext)
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        synchronized (this.lock) {
            if (!streamWriter.isUserClosed()
                    && streamWriter.isClosed()
                    && recreateCount.getAndIncrement() < MAX_RECREATE_COUNT) {
                streamWriter = createStreamWriter(streamWriter.getStreamName());
                this.error = null;
            }
            // If earlier appends have failed, we need to reset before continuing.
            if (this.error != null) {
                throw this.error;
            }
        }
        // Append asynchronously for increased throughput.
        ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
        ApiFutures.addCallback(
                future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

        // Increase the count of in-flight requests.
        inflightRequestCount.register();
    }

    public void cleanup() {
        // Wait for all in-flight requests to complete.
        inflightRequestCount.arriveAndAwaitAdvance();

        client.close();
        // Close the connection to the server.
        streamWriter.close();

        // Verify that no error occurred in the stream.
        synchronized (this.lock) {
            if (this.error != null) {
                throw this.error;
            }
        }
    }

    static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

        private static final Logger logger = LoggerFactory.getLogger(AppendCompleteCallback.class);

        private final DataWriter parent;
        private final AppendContext appendContext;

        public AppendCompleteCallback(DataWriter parent, AppendContext appendContext) {
            this.parent = parent;
            this.appendContext = appendContext;
        }

        public void onSuccess(AppendRowsResponse response) {
            logger.trace("Append success");
            this.parent.recreateCount.set(0);
            done();
        }

        public void onFailure(Throwable throwable) {
            if (throwable instanceof Exceptions.AppendSerializationError) {
                Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) throwable;
                Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
                if (rowIndexToErrorMessage.size() > 0) {
                    // Omit the faulty rows
                    JSONArray dataNew = new JSONArray();
                    for (int i = 0; i < appendContext.data.length(); i++) {
                        if (!rowIndexToErrorMessage.containsKey(i)) {
                            dataNew.put(appendContext.data.get(i));
                        } else {
                            // process faulty rows by placing them on a dead-letter-queue, for instance
                        }
                    }

                    // Retry the remaining valid rows, but using a separate thread to
                    // avoid potentially blocking while we are in a callback.
                    if (dataNew.length() > 0) {
                        try {
                            this.parent.append(new AppendContext(dataNew));
                        } catch (Descriptors.DescriptorValidationException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    // Mark the existing attempt as done since we got a response for it
                    done();
                    return;
                }
            }

            boolean resendRequest = false;
            if (throwable instanceof Exceptions.MaximumRequestCallbackWaitTimeExceededException) {
                resendRequest = true;
            } else if (throwable instanceof Exceptions.StreamWriterClosedException) {
                if (!parent.streamWriter.isUserClosed()) {
                    resendRequest = true;
                }
            }
            if (resendRequest) {
                // Retry this request.
                try {
                    this.parent.append(new AppendContext(appendContext.data));
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                // Mark the existing attempt as done since we got a response for it
                done();
                return;
            }

            synchronized (this.parent.lock) {
                if (this.parent.error == null) {
                    Exceptions.StorageException storageException = Exceptions.toStorageException(throwable);
                    this.parent.error =
                            (storageException != null) ? storageException : new RuntimeException(throwable);
                }
            }
            done();
        }

        private void done() {
            // Reduce the count of in-flight requests.
            this.parent.inflightRequestCount.arriveAndDeregister();
        }
    }
}