# Salesforce Bulk input plugin for Embulk

Salesforce Bulk API の一括クエリ結果を取得します。

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **userName**: Salesforce user name.(string, required)
- **password**: Salesforce password.(string, required)
- **authEndpointUrl**: Salesforce login endpoint URL.(string, default is "https://login.salesforce.com/services/Soap/u/39.0")
- **objectType**: object type of JobInfo.(string, required)
    - Usually same as query's object.(If **querySelectFrom** is `(snip) FROM xxx (snip)` then **dataType** is `xxx`)
- **pollingIntervalMillisecond**: polling interval millisecond.(string, default is 30000)
- **querySelectFrom**: part of query. SELECT and FROM.(string, required)
- **queryWhere**: part of query. WHERE.(string, default is "")
- **queryOrder**: part of query. ORDER BY.(string, default is "")
- **columns**: schema config.(SchemaConfig, required)
- **startRowMarkerName**: 開始レコードを特定するための目印とするカラム名を指定する.(String, default is null)
- **start_row_marker**: 抽出条件に、『カラム「startRowMarkerName」がこの値よりも大きい』を追加する.(string, default is null)
- **queryAll**: if true, uses the queryAll operation so that deleted records are returned.(boolean, default is false)


### More information about **objectType**:

**objectType** is field of JobInfo. See: [JobInfo | Bulk API Developer Guide | Salesforce Developers](https://developer.salesforce.com/docs/atlas.en-us.206.0.api_asynch.meta/api_asynch/asynch_api_reference_jobinfo.htm)

These documents will aid in understanding.

- [Create a Job | Bulk API Developer Guide | Salesforce Developers](https://developer.salesforce.com/docs/atlas.en-us.206.0.api_asynch.meta/api_asynch/asynch_api_jobs_create.htm)
- [Add a Batch to a Job | Bulk API Developer Guide | Salesforce Developers](https://developer.salesforce.com/docs/atlas.en-us.206.0.api_asynch.meta/api_asynch/asynch_api_batches_create.htm)
- [Use Bulk Query | Bulk API Developer Guide | Salesforce Developers](https://developer.salesforce.com/docs/atlas.en-us.206.0.api_asynch.meta/api_asynch/asynch_api_using_bulk_query.htm)


## Example

### query で指定したものをすべて抽出

```yaml
in:
  type: salesforce_bulk
  userName: USER_NAME
  password: PASSWORD
  authEndpointUrl: https://login.salesforce.com/services/Soap/u/39.0
  objectType: Account
  pollingIntervalMillisecond: 5000
  querySelectFrom: SELECT Id,Name,LastModifiedDate FROM Account
  queryWhere: Name like 'Test%'
  queryOrder: Name desc
  columns:
  - {type: string, name: Id}
  - {type: string, name: Name}
  - {type: timestamp, name: LastModifiedDate, format: '%FT%T.%L%Z'}
```

### 前回取得時点から変更があったオブジェクトのみ取得

startRowMarkerName に LastModifiedDate を指定したうえで、
-o オプションを指定して embulk を実行する。

#### config.yaml

```yaml
in:
  type: salesforce_bulk
  userName: USER_NAME
  password: PASSWORD
  authEndpointUrl: https://login.salesforce.com/services/Soap/u/39.0
  objectType: Account
  pollingIntervalMillisecond: 5000
  querySelectFrom: SELECT Id,Name,LastModifiedDate FROM Account
  queryOrder: Name desc
  columns:
  - {type: string, name: Id}
  - {type: string, name: Name}
  - {type: timestamp, name: LastModifiedDate, format: '%FT%T.%L%Z'}
  startRowMarkerName: LastModifiedDate
```

#### 実行コマンド

```sh
embulk run config.yaml -o config.yaml
```

## TODO

- エラーログ出力を真面目にやる
- guess 対応
- 効率化

## Build

```
$ ./gradlew gem
```
