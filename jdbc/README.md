<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# JDBC Driver for YCSB
This driver enables YCSB to work with databases accessible via the JDBC protocol.

## Getting Started
### 1. Start your database
This driver will connect to databases that use the JDBC protocol, please refer to your databases documentation on information on how to install, configure and start your system.

### 2. Set up YCSB
You can clone the YCSB project and compile it to stay up to date with the latest changes. Or you can just download the latest release and unpack it. Either way, instructions for doing so can be found here: https://github.com/brianfrankcooper/YCSB.

### 3. Configure your database and table.
You can name your database what ever you want, you will need to provide the database name in the JDBC connection string.

You can name your table whatever you like also, but it needs to be specified using the YCSB core properties, the default is to just use 'usertable' as the table name.

The expected table schema will look similar to the following, syntactical differences may exist with your specific database:

```sql
CREATE TABLE usertable (
	YCSB_KEY VARCHAR(255) PRIMARY KEY,
	FIELD0 TEXT, FIELD1 TEXT,
	FIELD2 TEXT, FIELD3 TEXT,
	FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT,
	FIELD8 TEXT, FIELD9 TEXT
);
```

Key take aways:

* The primary key field needs to be named YCSB_KEY
* The other fields need to be prefixed with FIELD and count up starting from 1
* Add the same number of FIELDs as you specify in the YCSB core properties, default is 10.
* The type of the fields is not so important as long as they can accept strings of the length that you specify in the YCSB core properties, default is 100.

#### JdbcDBCreateTable Utility
YCSB has a utility to help create your SQL table. NOTE: It does not support all databases flavors, if it does not work for you, you will have to create your table manually with the schema given above. An example usage of the utility:

```sh
java -cp YCSB_HOME/jdbc-binding/lib/jdbc-binding-0.4.0.jar:mysql-connector-java-5.1.37-bin.jar site.ycsb.db.JdbcDBCreateTable -P db.properties -n usertable
```

Hint: you need to include your Driver jar in the classpath as well as specify JDBC connection information via a properties file, and a table name with ```-n```. 

Simply executing the JdbcDBCreateTable class without any other parameters will print out usage information.

### 4. Configure YCSB connection properties
You need to set the following connection configurations:

```sh
db.driver=com.mysql.jdbc.Driver
db.url=jdbc:mysql://127.0.0.1:3306/ycsb
db.user=admin
db.passwd=admin
```

Be sure to use your driver class, a valid JDBC connection string, and credentials to your database.

For connection fail-over in a DBMS cluster specify the connection string as follows (example based on Postgres):

```sh
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://IP1:PORT1,IP2:PORT2,IP3:PORT3/ycsb
db.user=admin
db.passwd=admin
```

For using multiple shards in a DBMS cluster specify the connection string as follows by using `;`as delimiter (example based on PostgreSQL):

```sh
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://host1:port1/ycsb;jdbc:postgresql://host2:port2/ycsb
db.user=admin
db.passwd=admin
```

For SQL Server that uses `;` in URL parameter list, specify alternate URL shard delimiter.
```sh
db.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver
db.url=jdbc:sqlserver://sqlserver:1433;encrypt=false
db.urlsharddelim='_'
db.user=admin
db.passwd=admin
```

You can add these to your workload configuration or a separate properties file and specify it with ```-P``` or you can add the properties individually to your ycsb command with ```-p```.

### 5. Add your JDBC Driver to the classpath
There are several ways to do this, but a couple easy methods are to put a copy of your Driver jar in ```YCSB_HOME/jdbc-binding/lib/``` or just specify the path to your Driver jar with ```-cp``` in your ycsb command.

### 6. Running a workload
Before you can actually run the workload, you need to "load" the data first.

```sh
bin/ycsb load jdbc -P workloads/workloada -P db.properties -cp mysql-connector-java.jar
```

Then, you can run the workload:

```sh
bin/ycsb run jdbc -P workloads/workloada -P db.properties -cp mysql-connector-java.jar
```

## Configuration Properties

```sh
db.driver=com.mysql.jdbc.Driver             # The JDBC driver class to use.
db.url=jdbc:mysql://127.0.0.1:3306/ycsb     # The Database connection URL.
db.user=admin                               # User name for the connection.
db.passwd=admin                             # Password for the connection.
db.batchsize=1024                           # The batch size for doing batched inserts. Defaults to 0. Set to >0 to use batching.
jdbc.fetchsize=10                           # The JDBC fetch size hinted to the driver.
jdbc.autocommit=true                        # The JDBC connection auto-commit property for the driver.
jdbc.batchupdateapi=false                   # Use addBatch()/executeBatch() JDBC methods instead of executeUpdate() for writes (default: false)
jdbc.urlsharddelim=';'                      # Used specify alternate delimiter (default: `;`) 
db.batchsize=1000                           # The number of rows to be batched before commit (or executeBatch() when jdbc.batchupdateapi=true
```

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for all other YCSB core properties.

## JDBC Parameter to Improve Insert Performance

Some JDBC drivers support re-writing batched insert statements into multi-row insert statements. This technique can yield order of magnitude improvement in insert statement performance. To enable this feature:
- **db.batchsize** must be greater than 0.  The magniute of the improvement can be adjusted by varying **batchsize**. Start with a small number and increase at small increments until diminishing return in the improvement is observed. 
- set **jdbc.batchupdateapi=true** to enable batching.
- set JDBC driver specific connection parameter in **db.url** to enable the rewrite as shown in the examples below:
  * MySQL [rewriteBatchedStatements=true](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html) with `db.url=jdbc:mysql://127.0.0.1:3306/ycsb?rewriteBatchedStatements=true`
  * Postgres [reWriteBatchedInserts=true](https://jdbc.postgresql.org/documentation/head/connect.html#connection-parameters) with `db.url=jdbc:postgresql://127.0.0.1:5432/ycsb?reWriteBatchedInserts=true`
  * SQLServer [useBulkCopyForBatchInsert=true](https://learn.microsoft.com/en-us/sql/connect/jdbc/use-bulk-copy-api-batch-insert-operation?view=sql-server-ver16) with `-p db.urlsharddelim='_' -p db.url=jdbc:sqlserver://sqlserver:1433;encrypt=false;useBulkCopyForBatchInsert=true`
- Driver version requirements:
  * MariaDB JDBC Driver version needs to be less than 3.0.0 as [rewriteBatchedStatements](https://mariadb.com/kb/en/about-mariadb-connector-j/#removed-option) feature was removed.
  * SQL Server JDBC Driver version needs to be [9.2 or greater](https://techcommunity.microsoft.com/t5/sql-server-blog/jdbc-driver-9-2-for-sql-server-released/ba-p/2108693) 

## JDBC Parameters to Control Column Types and Contents
- `-p jdbc.ycsbkeyprefix` 
  - `true` by default inserts `YCSB_KEY` as a string.  The column will contain for example `user1`,`user2`,`user3`
  - `false` inserts `YCSB_KEY` as a numeric numeric column.  The column will contain for example `1`,`2`,`3`.
- `-p jdbc.prependtimestamp`
  - `false` by default inserts / updates `FIELD[*]` with random characters.  The column will contain for example `'=b#,n'S1 N75.48Q14.>.*`.
  - `true` inserts `FIELD[*]` with microsecond timestamp, space, followed by the random characters.  For example, `2024-01-29 23:32:34.123456 '=b#,n'S1 N75.48Q14.>.*`

# Statement Types

Batch and Multi row statements are used to increase the throughtput.  Each call to a database takes some time.  Instead of performing a single operation per call, multiple operations can be performed during that single call.  

NOTE: Can't use shard connection as inlist and batch could have key from different shards.

## Singleton Statement

This is the default statement issued that affects a single row.  

```
update set field1='foo' where ycsb_key = 1;
```

In Java, the return value of 0 means that row was not found and therefor not updated. 1 means the row was updated.


## Multi Row Statements

The following parameters control the number of entries in the SQL `IN` operation (also called `inlist`).  The value of 0 means don't use the feature whch is the default.   For example:

- `-p multiupdate=0` Don't use inlist for update statements

  ```
  update set field1='foo' from usertable where ycsb_key = 1;
  ```

- `-p multiinsert=1` One entry for the insert statements

  ```
  update set field1='foo' from usertable where ycsb_key in (1);
  ```

- `-p multidelete=2` Two entries for delete statements

  ```
  delete from usertable where ycsb_key in (1,2);
  ```

Note how single `set` parameter affect all of the rows.

In Java, the return value indicates the number of rows updated.

## Batch Statements

The following parameters control the number of statements batched.  0 means don't use the feature which is the default.  For example:

- `-p batchupdate=0` for update statements

  ```
  update set field1='foo' where ycsb_key = 1;
  ```

- `-p batchinsert=1` for insert statements

  ```
  update set field1='foo' where ycsb_key = 1; 
  ```

- `-p batchdelete=2` for delete statements

  ```
  update set field1='foo' from usertable where ycsb_key = 1; update set field2='foo' from usertable where ycsb_key in = 2;
  ```

Note how different `set` parameter is speficied for each of the row.

In Java, the return value indicates the number of rows updated.

## Batched and Multi Row Statements

`batch` and `inlist` can be combined together.

```
update set field1='foo' from usertable where ycsb_key in (1,2); update set field2='foo' from usertable where ycsb_key in (3,4);
```

In Java, the return value indicates the number of rows updated.