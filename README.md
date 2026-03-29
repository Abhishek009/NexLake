# NexLake

A modular ETL framework in Scala supporting YAML-based job configuration, Spark transformations, and flexible input/output sources.

## Features

- **YAML-driven ETL**: Define jobs, inputs, transforms, and outputs in YAML.
- **Custom Transform Scripts**: Supports SQL, Python, and Scala scripts for data transformation.
- **Multiple Engines**: Run jobs on Spark or DuckDB.
- **Robust Error Handling**: Logging and graceful failure for YAML parsing, Spark jobs, and database operations.

## Getting Started

### Prerequisites

- Java 11+
- Scala 2.12+
- Apache Spark 3.x
- Maven
- [DuckDB JDBC Driver](https://duckdb.org/docs/api/java/overview.html) (included by default)

## DuckDB Support

You can use DuckDB as an engine or as an output target in your ETL jobs. The framework supports reading from and writing to DuckDB tables directly.

### DuckDB Configuration Example

Add the following to your `example/conf/config.yml`:

```yaml
jdbc:
  duckdb_local:
    url: jdbc:duckdb:D:\SampleData\Output\duckdb\my_database.duckdb
    username: ""
    password: ""
```

### Example DuckDB Job YAML

See `example/yaml/job_FileToDuckDB.yml` or `example/yaml/job_FileToDuckDBTable.yml` for ready-to-use job definitions. Example snippet:

```yaml
jobName: FileToDuckDBTable
engine: duckdb
job:
  - input:
      df-name: input_data
      type: file
      identifier: local
      path: D:/SampleData/mock_data/sdf_sample_data.csv
      option: |
        delimiter=,
        header=true
  - transform:
      df-name: t1
      t_inputs: input_data
      query: "SELECT * FROM input_data LIMIT 10"
      output: out-01
  - output:
      df-name: out-01
      type: jdbc
      identifier: duckdb_local
      table: sdf_output_table
```

- `engine: duckdb` runs the job using the DuckDB engine.
- `type: jdbc` and `identifier: duckdb_local` specify DuckDB as the output.
- `url` is set in your config file, not in the job YAML.

## Running Jobs

Use the provided script to run your jobs. Example for DuckDB:

```bash
bash example/src/run_sdf.sh \
  --engine duckdb \
  --jobFile example/yaml/job_FileToDuckDBTable.yml \
  --jobConfig example/conf/config.yml \
  --configFile example/conf/meta.conf \
  --jar example/jars/SparkDataFlow-jar-with-dependencies.jar
```

- `--engine duckdb` runs the job using DuckDB.
- `--jobFile` points to your job YAML.
- `--jobConfig` and `--configFile` point to your configuration files.
- `--jar` is optional if you use the default location.

For more options, see the comments in `example/src/run_sdf.sh`.

## Configuration Files

- Main config: `example/conf/config.yml`
- Meta config: `example/conf/meta.conf`
- SDF config: `example/conf/sdf.conf`

## Example

See the `example/yaml/` folder for job YAMLs for DuckDB, MySQL, and more.

## MySQL Support

You can use MySQL as an engine or as an output target in your ETL jobs. The framework supports reading from and writing to MySQL tables directly.

### MySQL Configuration Example

Add the following to your `example/conf/config.yml`:

```yaml
jdbc:
  mysql:
    url: jdbc:mysql://localhost:3306/your_database
    username: your_username
    password: your_password
```

### Example MySQL Job YAML

See `example/yaml/job_FileToMysql.yml` for a ready-to-use job definition. Example snippet:

```yaml
jobName: FileToMysql
engine: spark
job:
  - input:
      df-name: mysql_customer_info
      type: file
      identifier: local
      path: D:/SampleData/mock_data/sdf_sample_data.csv
      option: |
        delimiter=,
        header=true
  - transform:
      df-name: t1
      t_inputs: mysql_customer_info
      query: "Select * from mysql_customer_info limit 10"
      output: out-01
  - output:
      df-name: out-01
      type: jdbc
      identifier: mysql
      table: sdf_framework_source_one
      schema: sdf_schema 
```

- `engine: spark` runs the job using the Spark engine.
- `type: jdbc` and `identifier: mysql` specify MySQL as the output.
- `url`, `username`, and `password` are set in your config file, not in the job YAML.

## Running MySQL Jobs

Use the provided script to run your MySQL jobs. Example:

```bash
bash example/src/run_sdf.sh \
  --engine spark \
  --jobFile example/yaml/job_FileToMysql.yml \
  --jobConfig example/conf/config.yml \
  --configFile example/conf/meta.conf \
  --jar example/jars/SparkDataFlow-jar-with-dependencies.jar
```

- `--engine spark` runs the job using Spark.
- `--jobFile` points to your job YAML.
- `--jobConfig` and `--configFile` point to your configuration files.
- `--jar` is optional if you use the default location.

For more options, see the comments in `example/src/run_sdf.sh`.

## Troubleshooting

- Ensure all file paths in your YAML and config files are correct.
- Check permissions for file access and database connections.
- Review logs for detailed error messages and stack traces.

## Contributing

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Make your changes and commit them.
4. Push to your fork and submit a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

