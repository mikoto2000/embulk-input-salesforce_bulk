package org.embulk.input.salesforce_bulk;

import com.sforce.async.AsyncApiException;
import com.sforce.ws.ConnectionException;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.embulk.config.TaskReport;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.timestamp.TimestampFormatter;

import org.slf4j.Logger;

public class SalesforceBulkInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        // 認証用エンドポイントURL
        @Config("authEndpointUrl")
        @ConfigDefault("\"https://login.salesforce.com/services/Soap/u/39.0\"")
        public String getAuthEndpointUrl();

        // ユーザー名
        @Config("userName")
        public String getUserName();

        // パスワード
        @Config("password")
        public String getPassword();

        // オブジェクトタイプ
        @Config("objectType")
        public String getObjectType();

        // SOQL クエリ文字列 SELECT, FROM
        @Config("querySelectFrom")
        public String getQuerySelectFrom();

        // SOQL クエリ文字列 WHERE
        @Config("queryWhere")
        @ConfigDefault("null")
        public Optional<String> getQueryWhere();

        // SOQL クエリ文字列 ORDER BY
        @Config("queryOrder")
        @ConfigDefault("null")
        public Optional<String> getQueryOrder();

        // 圧縮設定
        @Config("isCompression")
        @ConfigDefault("true")
        public Boolean getCompression();

        // ポーリング間隔(ミリ秒)
        @Config("pollingIntervalMillisecond")
        @ConfigDefault("30000")
        public int getPollingIntervalMillisecond();

        // スキーマ情報
        @Config("columns")
        public SchemaConfig getColumns();

        // next config のための最終レコード判定用カラム名
        @Config("startRowMarkerName")
        @ConfigDefault("null")
        public Optional<String> getStartRowMarkerName();

        // next config のための最終レコード値
        @Config("start_row_marker")
        @ConfigDefault("null")
        public Optional<String> getStartRowMarker();

        @Config("queryAll")
        @ConfigDefault("false")
        public Boolean getQueryAll();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        String getDefaultTimestampFormat();

        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();

    }

    private Logger log = Exec.getLogger(SalesforceBulkInputPlugin.class);

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    private final Map<String, TimestampFormatter> timestampFormatters = new HashMap<>();


    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);

        for (org.embulk.util.config.units.ColumnConfig columnConfig : task.getColumns().getColumns()) {
            log.info("name: {}, type: {}.", columnConfig.getName(), columnConfig.getType());
            log.info("IKUYO.");
            if (columnConfig.getType().toString().equals("timestamp")) {
                log.info("KITAYO.");
                final TimestampColumnOption columnOption = configMapper.map(columnConfig.getOption(), TimestampColumnOption.class);
                log.info("columnOption: {}", columnOption);

                TimestampFormatter formatter = TimestampFormatter
                    .builder(columnOption.getFormat().orElse(task.getDefaultTimestampFormat()))
                    .setDefaultZoneFromString(columnOption.getTimeZoneId().orElse(task.getDefaultTimeZoneId()))
                    .setDefaultDateFromString(columnOption.getDate().orElse(task.getDefaultDate()))
                    .build();
                log.info("formatter: {}", formatter);

                timestampFormatters.put(columnConfig.getName(), formatter);
            }
        }

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        ConfigDiff returnConfigDiff = resume(task.dump(), schema, taskCount, control);
        return returnConfigDiff;
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        List<TaskReport> taskReportList =
                control.run(taskSource, schema, taskCount);

        // start_row_marker を ConfigDiff にセット
        ConfigDiff configDiff = Exec.newConfigDiff();
        for (TaskReport taskReport : taskReportList) {
            final String label = "start_row_marker";
            final String startRowMarker = taskReport.get(String.class, label, null);
            if (startRowMarker != null) {
                configDiff.set(label, startRowMarker);
            }
        }
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        BufferAllocator allocator = Exec.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        // start_row_marker 取得のための前準備
        String start_row_marker = null;
        TaskReport taskReport = Exec.newTaskReport();

        log.info("Try login to '{}'.", task.getAuthEndpointUrl());
        try (SalesforceBulkWrapper sfbw = new SalesforceBulkWrapper(
                task.getUserName(),
                task.getPassword(),
                task.getAuthEndpointUrl(),
                task.getCompression(),
                task.getPollingIntervalMillisecond(),
                task.getQueryAll())) {

            log.info("Login success.");

            // クエリの作成
            String querySelectFrom = task.getQuerySelectFrom();
            String queryWhere = task.getQueryWhere().orElse("");
            String queryOrder = task.getQueryOrder().orElse("");
            String column = task.getStartRowMarkerName().orElse(null);
            String value = task.getStartRowMarker().orElse(null);

            String query;
            query = querySelectFrom;

            if (!queryWhere.isEmpty()) {
                queryWhere = " WHERE " + queryWhere;
            }

            if (column != null && value != null) {
                if (queryWhere.isEmpty()) {
                    queryWhere += " WHERE ";
                } else {
                    queryWhere += " AND ";
                }

                queryWhere += column + " > " + value;
            }

            query += queryWhere;

            if (!queryOrder.isEmpty()) {
                query += " ORDER BY " + queryOrder;
            }

            log.info("Send request : '{}'", query);

            List<Map<String, String>> queryResults = sfbw.syncQuery(
                    task.getObjectType(), query);

            for (Map<String, String> row : queryResults) {
                // Visitor 作成
                ColumnVisitor visitor = new ColumnVisitorImpl(row, task, pageBuilder, timestampFormatters);

                // スキーマ解析
                schema.visitColumns(visitor);

                // 編集したレコードを追加
                pageBuilder.addRecord();
            }
            pageBuilder.finish();

            // 取得した値の最大値を start_row_marker に設定
            if (column != null) {
                start_row_marker = queryResults.stream()
                    .map(item -> item.get(column))
                    .max(Comparator.naturalOrder()).orElse(null);

                if (start_row_marker == null) {
                    taskReport.set("start_row_marker", value);
                } else {
                    taskReport.set("start_row_marker", start_row_marker);
                }
            }
        } catch (ConnectionException|AsyncApiException|InterruptedException|IOException e) {
            log.error("{}", e.getClass(), e);
            throw new RuntimeException("SalesforceBulkWrapperError");
        }

        return taskReport;
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    class ColumnVisitorImpl implements ColumnVisitor {
        private final Map<String, String> row;
        private final PageBuilder pageBuilder;
        private final Map<String, TimestampFormatter> timestampFormatters;

        ColumnVisitorImpl(Map<String, String> row, PluginTask task, PageBuilder pageBuilder, Map<String, TimestampFormatter> timestampFormatters) {
            this.row = row;
            this.pageBuilder = pageBuilder;
            this.timestampFormatters = timestampFormatters;
        }

        @Override
        public void booleanColumn(Column column) {
            String value = row.get(column.getName());
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setBoolean(column, Boolean.parseBoolean(value));
            }
        }

        @Override
        public void longColumn(Column column) {
            String value = row.get(column.getName());
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                try {
                    pageBuilder.setLong(column, Long.parseLong(value));
                } catch (NumberFormatException e) {
                    log.error("NumberFormatError: Row: {}", row);
                    log.error("{}", e);
                    pageBuilder.setNull(column);
                }
            }
        }

        @Override
        public void doubleColumn(Column column) {
            String value = row.get(column.getName());
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                try {
                    pageBuilder.setDouble(column, Double.parseDouble(value));
                } catch (NumberFormatException e) {
                    log.error("NumberFormatError: Row: {}", row);
                    log.error("{}", e);
                    pageBuilder.setNull(column);
                }
            }
        }

        @Override
        public void stringColumn(Column column) {
            String value = row.get(column.getName());
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setString(column, value);
            }
        }

        @Override
        public void jsonColumn(Column column) {
            throw new UnsupportedOperationException("This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");
        }

        @Override
        public void timestampColumn(Column column) {
            String value = row.get(column.getName());
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                TimestampFormatter formatter = timestampFormatters.get(column.getName());
                if (formatter == null) {
                    throw new IllegalStateException("No timestamp formatter found for column: " + column.getName());
                }
                Instant timestamp = formatter.parse(value);
                pageBuilder.setTimestamp(column, timestamp);
            }
        }
    }

    private interface TimestampColumnOption extends org.embulk.util.config.Task {
        @Config("timezone")
        @ConfigDefault("null")
        java.util.Optional<String> getTimeZoneId();

        @Config("format")
        @ConfigDefault("null")
        java.util.Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        java.util.Optional<String> getDate();
    }
}
