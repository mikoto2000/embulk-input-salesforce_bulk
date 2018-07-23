# Salesforce Bulk input plugin for Embulk

Salesforce Bulk API の一括クエリ結果を取得します。

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Configuration

- **userName**: Salesforce user name.(string, required)
- **password**: Salesforce password.(string, required)
- **authEndpointUrl**: Salesforce login endpoint URL.(string, required)
- **objectType**: object type of JobInfo.(string, required)
- **pollingIntervalMillisecond**: polling interval millisecond.(string, default is 30000)
- **querySelectFrom**: part of query. SELECT and FROM. 指定がないか、もしくは空文字の場合はcolumnsとobjectTypeから自動生成する. 自動生成の際には、columnの要素にselectが記載されている場合はnameではなくそちらを利用する.(string, default is null)
- **queryWhere**: part of query. WHERE.(string, default is "")
- **queryOrder**: part of query. ORDER BY.(string, default is "")
- **columns**: schema config.(SchemaConfig, required)
- **startRowMarkerName**: 開始レコードを特定するための目印とするカラム名を指定する.(String, default is null)
- **start_row_marker**: 抽出条件に、『カラム「startRowMarkerName」がこの値よりも大きい』を追加する.(string, default is null)
- **queryAll**: if true, uses the queryAll operation so that deleted records are returned.(boolean, default is false)
- **showAllObjectTypesByGuess**: if true, add all names and labels of objectTypes to 'objectTypes'. (boolean, default is false)
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

### querySelectFromを自動生成させ、さらに一部を別名で取得

```yaml
in:
  type: salesforce_bulk
  userName: USER_NAME
  password: PASSWORD
  authEndpointUrl: https://login.salesforce.com/services/Soap/u/39.0
  objectType: Account
  pollingIntervalMillisecond: 5000
  queryOrder: Name desc
  columns:
  - {type: string, name: Id}
  - {type: string, name: Name}
  - {type: string, name: Father_Name,  select: Father__r.Name}
  - {type: string, name: Grandpa_Name, select: Father__r.Father__r.Name}
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
- 効率化

## Build

```
$ ./gradlew gem
```
