package org.embulk.input.salesforce_bulk;

import com.google.common.base.Optional;

import java.io.IOException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.sforce.async.AsyncApiException;
import com.sforce.ws.ConnectionException;

import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

import org.embulk.spi.time.Timestamp;

public class SalesforceBulkInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        // 認証用エンドポイントURL
        @Config("authEndpointUrl")
        @ConfigDefault("http://login.salesforce.com/")
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

        // SOQL クエリ文字列
        @Config("query")
        public String getQuery();

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

        // 謎。バッファアロケーターの実装を定義？
        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<CommitReport> successCommitReports)
    {
    }

    @Override
    public CommitReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        try (SalesforceBulkWrapper sfbw = new SalesforceBulkWrapper(
                task.getUserName(),
                task.getPassword(),
                task.getAuthEndpointUrl(),
                task.getCompression(),
                task.getPollingIntervalMillisecond())) {

            List<Map<String, String>> queryResults = sfbw.syncQuery(
                    task.getObjectType(), task.getQuery());

            for (Map<String, String> row : queryResults) {
                // Visitor 作成
                // TODO: 毎回作るのをやめたい。
                ColumnVisitor visitor = new ColumnVisitorImpl(row, task, pageBuilder);

                // スキーマ解析
                schema.visitColumns(visitor);

                // 編集したレコードを追加
                pageBuilder.addRecord();
            }
            pageBuilder.finish();
        } catch (ConnectionException|AsyncApiException|InterruptedException|IOException e) {
            e.printStackTrace();
        }

        return Exec.newCommitReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    class ColumnVisitorImpl implements ColumnVisitor {
        private final Map<String, String> row;
        private final PluginTask task;
        private final PageBuilder pageBuilder;

        ColumnVisitorImpl(Map<String, String> row, PluginTask task, PageBuilder pageBuilder) {
            this.row = row;
            this.task = task;
            this.pageBuilder = pageBuilder;
        }

        @Override
        public void booleanColumn(Column column) {
            Boolean value = new Boolean(row.get(column.getName()));
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setBoolean(column, value);
            }
        }

        @Override
        public void longColumn(Column column) {
            Long value = new Long(row.get(column.getName()));
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setLong(column, value);
            }
        }

        @Override
        public void doubleColumn(Column column) {
            Double value = new Double(row.get(column.getName()));
            if (value == null) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setDouble(column, value);
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
        public void timestampColumn(Column column) {
            // TODO: 毎回取りに行くのをやめたい。
            String format = task.getColumns().getColumn(
                    column.getIndex()).getOption().get(String.class, "format");

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

            String dateStr = row.get(column.getName());
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateStr, formatter);
            if (zonedDateTime == null) {
                pageBuilder.setNull(column);
            } else {
                Date d = Date.from(zonedDateTime.toInstant());
                Timestamp timestamp = Timestamp.ofEpochMilli(d.getTime());
                pageBuilder.setTimestamp(column, timestamp);
            }
        }
    }
}
