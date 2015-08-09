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
- **authEndpointUrl**: Salesforce login endpoint URL.(string, required)
- **objectType**: object type of JobInfo.(string, required)
- **query**: query string.(string, required)
- **pollingIntervalMillisecond**: polling interval millisecond.(string, default is 30000)
- **columns**: schema config.(SchemaConfig, required)

## Example

```yaml
in:
  type: salesforce_bulk
  userName: USER_NAME
  password: PASSWORD
  authEndpointUrl: https://login.salesforce.com/services/Soap/u/34.0
  objectType: Account
  pollingIntervalMillisecond: 5000
  query: SELECT Id,Name,CreatedDate FROM Account ORDER BY Id
  columns:
  - {type: string, name: Id}
  - {type: string, name: Name}
  - {type: timestamp, name: CreatedDate, format: 'yyyy-MM-dd''T''HH:mm:ss.SSSzzz'}
```

## TODO

- next config diff をどうにかする
- エラーログ出力を真面目にやる

## Build

```
$ ./gradlew gem
```
