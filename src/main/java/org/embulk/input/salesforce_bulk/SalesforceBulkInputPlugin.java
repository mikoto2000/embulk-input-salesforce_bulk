package org.embulk.input.salesforce_bulk;

import com.google.common.base.Optional;
import com.sforce.async.AsyncApiException;
import com.sforce.ws.ConnectionException;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.Timestamps;
import org.slf4j.Logger;

import org.slf4j.Logger;

import com.sforce.soap.partner.Field;

public class SalesforceBulkInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task, TimestampParser.Task
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
        @ConfigDefault("null")        
        public Optional<String> getQuerySelectFrom();

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

        // 謎。バッファアロケーターの実装を定義？
        @ConfigInject
        public BufferAllocator getBufferAllocator();

        @Config("queryAll")
        @ConfigDefault("false")
        public Boolean getQueryAll();
    }

    private Logger log = Exec.getLogger(SalesforceBulkInputPlugin.class);

    public static String castTypeName(String typename){
        //  boolean, long, double, string, timestamp, json (through reference chain: java.util.ArrayList[0])
        /*
          "picklist"
          "multipicklist"
          "combobox"
          "reference"
          "base64"
          "boolean"
          "currency"
          "textarea"
          "int"
          "double"
          "percent"
          "phone"
          "id"
          "date"
          "datetime"
          "time"
          "url"
          "email"
          "encryptedstring"
          "anyType"
        */
        switch(typename){
        case "boolean":
        case "double" :
            return typename;
        case "int" :
            return "long";
        case "date" :
        case "datetime" :
        case "time" :
            return "timestamp";
        default:
            return "string";
        }
    }
    
    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

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
        PluginTask task = taskSource.loadTask(PluginTask.class);

        BufferAllocator allocator = task.getBufferAllocator();
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
            String querySelectFrom = task.getQuerySelectFrom().or("").trim();
            String queryWhere = task.getQueryWhere().or("");
            String queryOrder = task.getQueryOrder().or("");
            String column = task.getStartRowMarkerName().orNull();
            String value = task.getStartRowMarker().orNull();

            String query;

            // select文の記載がない場合は自動生成
            if(querySelectFrom.isEmpty()){
                querySelectFrom = this.guessQuerySelectFromByTask(task);
            }
            
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
                ColumnVisitor visitor = new ColumnVisitorImpl(row, task, pageBuilder);

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
        }

        return taskReport;
    }

    public static String guessQuerySelectFromByTask(PluginTask task){
        String from = task.getObjectType();
        return guessQuerySelectFromByConfigSourceList(from,task.getColumns().getColumns().stream().map(cc->cc.getConfigSource()).collect(Collectors.toList()));
    }
        
    public static String guessQuerySelectFromByConfigSourceList(String from, List<ConfigSource> cslist)
    {
        List<String> select_xs = new ArrayList<String>();
        ConfigSource[] csarr = cslist.toArray(new ConfigSource[0]);
        for(ConfigSource src : csarr){
            if(src != null && src.has("name")){
                String select = src.get(String.class, "name");
                if(src.has("select")){
                   select = src.get(String.class, "select");
                }
                select_xs.add(select);
            }
        }
                
        return
            "SELECT "
            + String.join(",", select_xs.toArray(new String[0]))
            + " FROM " + from;
    }

    public static String guessFormat(String type){
        switch(type){
        case "date":
            return "%Y-%m-%d";
        case "datetime":
            return "%Y-%m-%dT%H:%M:%S.%N%z";
        case "time":
            return "%H:%M:%S.%N%z";
        default:
            return null;
        }
    }
    
    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        String querySelectFrom = task.getQuerySelectFrom().or("");
        SchemaConfig sc = task.getColumns();
        List<ColumnConfig> cclist = (sc == null) ? new ArrayList<ColumnConfig>() : sc.getColumns();
        Map<String,ColumnConfig> ccmap = new HashMap<String,ColumnConfig>();
        java.util.Iterator<ColumnConfig> itr = cclist.iterator();
        while(itr.hasNext()){
            ColumnConfig x = itr.next();
            ccmap.put(x.getName(),x);
        }
        String[] columnNames = (cclist == null) ? new String[0] : cclist.stream().map(c->c.getName()).collect(Collectors.toList()).toArray(new String[0]);
        
        try{
            SalesforceBulkWrapper sfbw =
                new SalesforceBulkWrapper(task.getUserName(),
                                          task.getPassword(),
                                          task.getAuthEndpointUrl(),
                                          task.getCompression(),
                                          task.getPollingIntervalMillisecond(),
                                          task.getQueryAll()
                                          );

            Field[] fs = sfbw.getFieldsOf(task.getObjectType());
            
            Map<String,Field> col_info_map = new HashMap<String,Field>();
            
            if(columnNames == null || columnNames.length < 1){
                columnNames= Arrays.stream(fs).map(x->x.getName()).collect(Collectors.toList()).toArray(new String[0]);
            }

            for(Field f : fs){
                col_info_map.put(f.getName(),f);
            }

            List<ConfigSource> srcs = new ArrayList<ConfigSource>();
            for(String name : columnNames){
                Field f = col_info_map.get(name);
                ColumnConfig cc = ccmap.get(name);
                ConfigSource src = cc.getConfigSource();
                if(f != null){
                    String typeOnSfdc = ""+f.getType();
                    String type = this.castTypeName(typeOnSfdc);
                    src.set("type",type);
                    
                    String label = f.getLabel();
                    if(label!=null || label.equals("")){
                        src.set("label",label);
                    }
                    if(f.getLength() > 0){
                        src.set("size", ""+f.getLength());
                    }
                    if(f.getPrecision() > 0){
                        src.set("precision",""+f.getPrecision());
                    }

                    // start guess format
                    ConfigSource option = cc.getOption();
                    if(option != null){
                        String format = null;
                        if(option.has("format")){
                            format = option.get(String.class,"format");
                        }
                        format = format == null ? "" : format;
                        if(format.isEmpty()){
                            format = this.guessFormat(typeOnSfdc);
                        }
                        format = format == null ? "" : format;
                        if(!format.isEmpty()){
                            // set format
                            src.set("format",format);
                        }
                    }
                    // end guess format
                }
                srcs.add(src);
            }
            
            if(querySelectFrom == null || querySelectFrom.trim().equals("")){
                querySelectFrom = this.guessQuerySelectFromByConfigSourceList(task.getObjectType(),srcs);
            }

            return Exec.newConfigDiff()
                .set("columns", srcs)
                .set("querySelectFrom", querySelectFrom);
        } catch (ConnectionException|AsyncApiException e) {
            log.error("{}", e.getClass(), e);
        }

        return Exec.newConfigDiff();
    }

    class ColumnVisitorImpl implements ColumnVisitor {
        private final Map<String, String> row;
        private final TimestampParser[] timestampParsers;
        private final PageBuilder pageBuilder;

        ColumnVisitorImpl(Map<String, String> row, PluginTask task, PageBuilder pageBuilder) {
            this.row = row;
            this.pageBuilder = pageBuilder;

            this.timestampParsers = Timestamps.newTimestampColumnParsers(
                    task, task.getColumns());
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
                try {
                    Timestamp timestamp = timestampParsers[column.getIndex()]
                            .parse(value);
                    pageBuilder.setTimestamp(column, timestamp);
                } catch (TimestampParseException e) {
                    log.error("TimestampParseError: Row: {}", row);
                    log.error("{}", e);
                    pageBuilder.setNull(column);
                }
            }
        }
    }
}
