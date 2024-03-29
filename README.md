## Iceberg Spark

This project is created to show examples of how Iceberg can be used for various data engineering use cases.


### Run with in memory jdbc catalog
```shell
unset CATALOG_URL
unset CATALOG_WAREHOUSE
./gradlew runTask
```


### Run with Catalog REST Server
We will run Iceberg REST catalog server first to store iceberg table metadata.

Start Catalog Server
```shell
docker run -p 8181:8181 -v $(pwd)/tmp:$(pwd)/tmp \
-e CATALOG_WAREHOUSE=$(pwd)/tmp/new-warehouse  \
-e CATALOG_URI=jdbc:sqlite:file:$(pwd)/tmp/iceberg_rest_mode=memory \
-d tabulario/iceberg-rest
```

We are ensuring the Catalog warehouse path is the same for both the spark process and the catalog server that runs as a container.

```
#not passing catalog url will default to in memory jdbc catalog
export CATALOG_URL=http://localhost:8181
export CATALOG_WAREHOUSE=$(pwd)/tmp/new-warehouse
export CATALOG_URI=jdbc:sqlite:file:$(pwd)/tmp/spark_rest_mode=memory
./gradlew runTask
```



### Query
We will query the dataset the same way we did with [trino-by-example](https://github.com/skhatri/trino-by-example)

#### Find total number of activities by account

``` 
select account, count(*) as activity_count from iceberg.finance.activity
group by account;
```

#### As of a specific snapshot

As of version 2024-03-02, what was transaction id txn10 labelled as?

```scala
  spark.sql(
  s"""select * from iceberg.finance.activity VERSION AS OF 'day1'
     |where txn_id='txn10'
     |""".stripMargin).show(2, truncate = false)
```

| account | txn_date   | txn_id | merchant         | amount | category  | last_updated        |
|---------|------------|--------|------------------|--------|-----------|---------------------|
| acc4    | 2024-03-02 | txn10  | Prouds Jewellery | 189.0  | Jewellery | 2024-03-02 00:00:00 |

The same record on day 5

```scala
  spark.sql(
  s"""select * from iceberg.finance.activity VERSION AS OF 'day5'
     |where txn_id='txn10'
     |""".stripMargin).show(2, truncate = false)
```

| account | txn_date   | txn_id | merchant         | amount | category | last_updated        |
|---------|------------|--------|------------------|--------|----------|---------------------|
| acc4    | 2024-03-02 | txn10  | Prouds Jewellery | 189.0  | Fashion  | 2024-03-08 00:00:00 |

#### Latest category

What is the latest category of transaction id txn10 and when was it last updated?

```scala 
  spark.sql(
  """select * from iceberg.finance.activity 
    |where txn_id='txn10'
    |""".stripMargin).show(2, truncate = false)
```

| account | txn_date   | txn_id | merchant         | amount | category | last_updated        |
|---------|------------|--------|------------------|--------|----------|---------------------|
| acc4    | 2024-03-02 | txn10  | Prouds Jewellery | 189.0  | Fashion  | 2024-03-08 00:00:00 |

#### Category Change over time

Acc5 bought something from Apple Store Sydney on 2021-03-05, how did the category for this transaction change over time?

```scala
  spark.sql(
  """select * 
    |from iceberg.finance.activity VERSION as of 'day3'
    |where account = 'acc5' and txn_date=cast('2024-03-05' as date) and merchant='Apple Store Sydney'""".stripMargin).show(2, truncate = false)
```

| account | txn_date   | txn_id | merchant           | amount | category | last_updated        |
|---------|------------|--------|--------------------|--------|----------|---------------------|
| acc5    | 2024-03-05 | txn44  | Apple Store Sydney | 1500.0 | Hardware | 2024-03-05 00:00:00 |

```scala
  spark.sql(
  """select *
    |from iceberg.finance.activity
    |where account = 'acc5' and txn_date=cast('2024-03-05' as date) and merchant='Apple Store Sydney'""".stripMargin).show(2, truncate = false)
```

| account | txn_date   | txn_id | merchant           | amount | category | last_updated        |
|---------|------------|--------|--------------------|--------|----------|---------------------|
| acc5    | 2024-03-05 | txn44  | Apple Store Sydney | 1500.0 | Phone    | 2024-03-09 00:00:00 |


### Write Audit Publish (WAP) with Iceberg
In this instance, our objective is to transfer data into a fresh table mirroring the schema of 'activity', which we dub 'activity2'. 
Initially, we import clean dataset from a CSV file located in the folder dated 2024-03-02. We initiate the process by configuring spark.wap.branch to 'feature_branch'.
After the data loading completes, a validation step kicks-in to ensure record integrity. We check for null values across all columns. Should the validation succeed, we advance the pointer to the primary branch corresponding to the WAP branch.
In cases where validation fails, we discard it. At this point, we delete the WAP branch regardless of the result.

Run this using the following command.

``` 
./gradlew runTask -Pname=WapIceberg
```
