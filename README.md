### Iceberg Spark
This project is created to show examples of how Iceberg can be used for various data engineering use cases.

We will run Iceberg REST catalog server first to store iceberg table metadata.

```shell
docker run -p 8181:8181 -it tabular/iceberg-rest
```

### Run Task with catalog

```
#not passing catalog url will default to in memory jdbc catalog
export CATALOG_URL=http://localhost:8181
./gradlew runApp
```

