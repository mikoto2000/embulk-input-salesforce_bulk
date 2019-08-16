package org.embulk.input.salesforce_bulk;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ContentType;
import com.sforce.async.CSVReader;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;

import com.sforce.soap.partner.PartnerConnection;

import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * SalesforceBulkWrapper.
 *
 * -- example:
 * <pre>
 * {@code
 * SalesforceBulkWrapper sfbw = new SalesforceBulkWrapper(
 *         USER_NAME,
 *         PASSWORD,
 *         AUTH_ENDPOINT_URL,
 *         IS_COMPRESSION,
 *         POLLING_INTERVAL_MILLISECOND);
 * List<Map<String, String>> results = sfbw.syncQuery(
 *         "Account", "SELECT Id, Name FROM Account ORDER BY Id");
 * sfbw.close();
 * }
 * </pre>
 */
public class SalesforceBulkWrapper implements AutoCloseable {

    // コネクション
    private PartnerConnection partnerConnection;
    private BulkConnection bulkConnection;

    // Bulk 接続設定
    private boolean isCompression;
    private int pollingIntervalMillisecond;
    private boolean queryAll;

    private static final String API_VERSION = "39.0";
    private static final String AUTH_ENDPOINT_URL_DEFAULT =
            "https://login.salesforce.com/services/Soap/u/" + API_VERSION;

    private static final boolean IS_COMPRESSION_DEFAULT = true;
    private static final int POLLING_INTERVAL_MILLISECOND_DEFAULT = 30000;
    private static final boolean QUERY_ALL_DEFAULT = false;

    /**
     * Constructor
     */
    public SalesforceBulkWrapper(String userName, String password)
            throws AsyncApiException, ConnectionException {
        this(userName,
                password,
                AUTH_ENDPOINT_URL_DEFAULT,
                IS_COMPRESSION_DEFAULT,
                POLLING_INTERVAL_MILLISECOND_DEFAULT,
                QUERY_ALL_DEFAULT);
    }

    /**
     * Constructor
     */
    public SalesforceBulkWrapper(
            String userName,
            String password,
            String authEndpointUrl,
            boolean isCompression,
            int pollingIntervalMillisecond,
            boolean queryAll)
            throws AsyncApiException, ConnectionException {

        partnerConnection = createPartnerConnection(
                authEndpointUrl,
                userName,
                password);
        bulkConnection = createBulkConnection(partnerConnection.getConfig());

        this.pollingIntervalMillisecond = pollingIntervalMillisecond;
        this.queryAll = queryAll;
    }

    public List<Map<String, String>> syncQuery(String objectType, String query)
            throws InterruptedException, AsyncApiException, IOException {

        // ジョブ作成
        JobInfo jobInfo = new JobInfo();
        jobInfo.setObject(objectType);
        if (queryAll) {
            jobInfo.setOperation(OperationEnum.queryAll);
        } else {
            jobInfo.setOperation(OperationEnum.query);
        }
        jobInfo.setContentType(ContentType.CSV);
        jobInfo = bulkConnection.createJob(jobInfo);

        // バッチ作成
        InputStream is = new ByteArrayInputStream(query.getBytes());
        BatchInfo batchInfo = bulkConnection.createBatchFromStream(jobInfo, is);

        // ジョブクローズ
        JobInfo closeJob = new JobInfo();
        closeJob.setId(jobInfo.getId());
        closeJob.setState(JobStateEnum.Closed);
        bulkConnection.updateJob(closeJob);

        // 実行状況取得
        batchInfo = waitBatch(batchInfo);
        BatchStateEnum state = batchInfo.getState();

        // 実行結果取得
        if (state == BatchStateEnum.Completed) {
            QueryResultList queryResultList =
                    bulkConnection.getQueryResultList(
                            batchInfo.getJobId(),
                            batchInfo.getId());
            return getQueryResultMapList(batchInfo, queryResultList);
        } else {
            throw new AsyncApiException(batchInfo.getStateMessage(), AsyncExceptionCode.InvalidBatch);
        }
    }

    private List<Map<String, String>> getQueryResultMapList(BatchInfo batchInfo,
            QueryResultList queryResultList)
            throws AsyncApiException, IOException {

        List<Map<String, String>> queryResults = new ArrayList<>();

        for (String queryResultId : queryResultList.getResult()) {
            CSVReader rdr =
                new CSVReader(bulkConnection.getQueryResultStream(
                            batchInfo.getJobId(),
                            batchInfo.getId(),
                            queryResultId), StandardCharsets.UTF_8.name());

            // バッチ作成時の CSV 制限は今回関係ないのですべて Integer.MAX_VALUE に設定。
            rdr.setMaxRowsInFile(Integer.MAX_VALUE);
            rdr.setMaxCharsInFile(Integer.MAX_VALUE);

            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                HashMap<String, String> rowMap = new HashMap<>(resultCols);
                for (int i = 0; i < resultCols; i++) {
                    rowMap.put(resultHeader.get(i), row.get(i));
                }
                queryResults.add(rowMap);
            }
        }
        return queryResults;
    }

    public void close() throws ConnectionException {
        partnerConnection.logout();
    }

    private PartnerConnection createPartnerConnection(
            String endpointUrl,
            String userName,
            String password)
            throws ConnectionException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userName);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(endpointUrl);

        return new PartnerConnection(partnerConfig);
    }

    private BulkConnection createBulkConnection(ConnectorConfig partnerConfig)
            throws AsyncApiException {

        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());

        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String restEndpoint = soapEndpoint.substring(
                0, soapEndpoint.indexOf("Soap/")) + "async/" + API_VERSION;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(isCompression);

        config.setTraceMessage(false);

        return new BulkConnection(config);
    }

    private BatchInfo waitBatch(BatchInfo batchInfo)
            throws InterruptedException, AsyncApiException {
        while(true) {
            Thread.sleep(pollingIntervalMillisecond);
            batchInfo = bulkConnection.getBatchInfo(
                    batchInfo.getJobId(),
                    batchInfo.getId());
            BatchStateEnum state = batchInfo.getState();
            if (state == BatchStateEnum.Completed ||
                    state == BatchStateEnum.Failed ||
                    state == BatchStateEnum.NotProcessed) {
                return batchInfo;
            }
        }
    }
}
