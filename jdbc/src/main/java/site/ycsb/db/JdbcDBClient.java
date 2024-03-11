/**
 * Copyright (c) 2010 - 2016 Yahoo! Inc., 2016, 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import site.ycsb.db.flavors.DBFlavor;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 *
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 *
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type TEXT. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 */
public class JdbcDBClient extends DB {

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";
  public static final String URL_SHARD_DELIM = "db.urlsharddelim";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** Default number of rows in multi row update. */
  public static final String USE_MULTI_UPDATE = "jdbc.usemultiupdate";
  public static final boolean USE_MULTI_UPDATE_DEFAULT = false;
  public static final String MULTI_UPDATE_SIZE = "jdbc.multiupdatesize";
  public static final String MULTI_UPDATE_SIZE_DEFAULT = "0";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

  /** Make YCSB_KEY field integer. **/
  public static final String JDBC_YCSB_KEY_STRING = "jdbc.ycsbkeyprefix";

  /** Prepend YY-MM-DD HH:MM:DD.123456 to the FEILD[*] data. **/
  public static final String JDBC_PREPEND_TIMESTAMP = "jdbc.prependtimestamp";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  /** SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY. */
  private boolean sqlansiScans = false;
  /** SQL Server before 2012: TOP n after the SELECT. */
  private boolean sqlserverScans = false;

  private List<Connection> conns;
  private boolean initialized = false;
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  // for multirow update
  private int multiUpdateSize;
  private int savedNumFields;
  private String savedUpdateKey;
  private PreparedStatement savedUpdateStatement;
  private OrderedFieldInfo savedFieldInfo;  
  private boolean autoCommit;
  private boolean batchUpdates;
  private boolean ycsbKeyStringType;
  private boolean ycsbPrependTimestamp;
  private String urlShardDelim = ";";
  private static final String DEFAULT_PROP = "";
  private static final String DEFAULT_URL_SHARD_DELIM = ";";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;
  private int numRowsInMultiInsert = 0;
  private int numRowsInMultiUpdate = 0;
  private int numRowsInMultiDelete = 0;
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;

  /**
   * Ordered field information for insert and update statements.
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);
    
    this.urlShardDelim = props.getProperty(URL_SHARD_DELIM, DEFAULT_URL_SHARD_DELIM);
    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    this.batchSize = getIntProperty(props, DB_BATCH_SIZE);
    this.multiUpdateSize = Integer.parseInt(props.getProperty(MULTI_UPDATE_SIZE, MULTI_UPDATE_SIZE_DEFAULT));

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    this.batchUpdates = getBoolProperty(props, JDBC_BATCH_UPDATES, false);
    this.ycsbKeyStringType = getBoolProperty(props, JDBC_YCSB_KEY_STRING, true);
    this.ycsbPrependTimestamp = getBoolProperty(props, JDBC_PREPEND_TIMESTAMP, false);

    try {
//  The SQL Syntax for Scan depends on the DB engine
//  - SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY
//  - SQL Server before 2012: TOP n after the SELECT
//  - others (MySQL,MariaDB, PostgreSQL before 8.4)
//  TODO: check product name and version rather than driver name
      if (driver != null) {
        if (driver.contains("sqlserver")) {
          sqlserverScans = true;
          sqlansiScans = false;
        }
        if (driver.contains("oracle")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        if (driver.contains("postgres")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        // The newInstance() call is a work around for some
        // broken Java implementations
        Class.forName(driver).newInstance();
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      // for a longer explanation see the README.md
      // semicolons aren't typically present in JDBC urls except for SQL Server, so we use them to delimit
      // multiple JDBC connections to shard across.  override with jdbc.urldelim for SQL Server
      final String[] urlArr = urls.split(this.urlShardDelim);
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        shardCount++;
        conns.add(conn);
      }

      System.out.println("Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

      this.dbFlavor = DBFlavor.fromJdbcUrl(urlArr[0]);
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    } catch (java.lang.InstantiationException e) {
      System.err.println("Error in database instantiation: " + e);
      throw new DBException(e);
    } catch (java.lang.IllegalAccessException e) {
      System.err.println("Error in database instantiation: " + e);
      throw new DBException(e);
    } 
    

    initialized = true;
  }

  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
        if (this.savedUpdateStatement != null) {
          for (int i=this.numRowsInMultiUpdate; i <=this.multiUpdateSize; i++) {
            setYcsbKey(this.savedUpdateStatement, this.savedNumFields + i, this.savedUpdateKey);
          }
          execMultiStmt(this.savedUpdateStatement, this.numRowsInMultiUpdate);
        }        
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }
    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
      throws SQLException {

    String insert = dbFlavor.createInsertStatement(insertType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert);
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
      throws SQLException {
    String read = dbFlavor.createReadStatement(readType, key);
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read);
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) {
      return readStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
      throws SQLException {
    String delete = dbFlavor.createDeleteStatement(deleteType, key);
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete);
    PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) {
      return deleteStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
      throws SQLException {
    String update = dbFlavor.createUpdateStatement(updateType, key);
    PreparedStatement updateStatement = getShardConnectionByKey(key).prepareStatement(update);
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, updateStatement);
    if (stmt == null) {
      return updateStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheMultiUpdateStatement(StatementType updateType, String key, int multiSize)
      throws SQLException {
    String update = dbFlavor.createMultiUpdateStatement(updateType, key, multiSize);
    PreparedStatement updateStatement = getShardConnectionByKey(key).prepareStatement(update);
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, updateStatement);
    if (stmt == null) {
      return updateStatement;
    }
    return stmt;
  }
  
  private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
      throws SQLException {
    String select = dbFlavor.createScanStatement(scanType, key, sqlserverScans, sqlansiScans);
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select);
    if (this.jdbcFetchSize > 0) {
      scanStatement.setFetchSize(this.jdbcFetchSize);
    }
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) {
      return scanStatement;
    }
    return stmt;
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      if (this.ycsbKeyStringType) {
        readStatement.setString(1, key);
      } else {
        readStatement.setInt(1, Integer.parseInt(key.substring(4)));
      }
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, "", getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      // SQL Server TOP syntax is at first
      if (sqlserverScans) {
        scanStatement.setInt(1, recordcount);
        if (this.ycsbKeyStringType) {
          scanStatement.setString(2, startKey);
        } else {
          scanStatement.setInt(2, Integer.parseInt(startKey.substring(4)));
        }        
      // FETCH FIRST and LIMIT are at the end
      } else {
        if (this.ycsbKeyStringType) {
          scanStatement.setString(1, startKey);
        } else {
          scanStatement.setInt(1, Integer.parseInt(startKey.substring(4)));
        }            
        scanStatement.setInt(2, recordcount);
      }
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private void setYcsbKey(PreparedStatement aStatement, int keyIndex, String key) throws SQLException {
    if (this.ycsbKeyStringType) {
      aStatement.setString(keyIndex, key);
    } else {
      // substring(4) skips the "user" in the typical key such as "user1234"
      aStatement.setInt(keyIndex, Integer.parseInt(key.substring(4)));
    }  
  }

  private Status execMultiStmt(PreparedStatement aStatement, int numRowsInMulti) throws SQLException {
    int result = aStatement.executeUpdate();
    if (numRowsInMulti > 0) {
      if (result == numRowsInMulti) {
        return Status.BATCHED_OK;
      } else {
        // not all rows were updated
        return Status.NOT_FOUND;
      }
    } else if (result == 1) {
      return Status.OK;
    } else if (result == 0) {
      return Status.NOT_FOUND;
    }
    return Status.UNEXPECTED_STATE;
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      // PreparedStatement updateStatement = cachedStatements.get(type);
      if (this.savedUpdateStatement == null) {
        this.savedNumFields = values.size();
        this.savedFieldInfo = getFieldInfo(values);
        StatementType type = new StatementType(StatementType.Type.UPDATE, tableName,
            this.savedNumFields, this.savedFieldInfo.getFieldKeys(), getShardIndexByKey(key));

        if (this.multiUpdateSize > 0) {
          this.savedUpdateStatement = createAndCacheMultiUpdateStatement(type, key, this.multiUpdateSize);
        } else {
          this.savedUpdateStatement = createAndCacheUpdateStatement(type, key);
        }
      }
      // set field1=xx, field2=xx
      int index = 1;
      for (String value: savedFieldInfo.getFieldValues()) {
        this.savedUpdateStatement.setString(index++, value);
      }

      // where ycsb_key = [?|(?,?)]
      setYcsbKey(this.savedUpdateStatement, index + numRowsInMultiUpdate, key);
      this.savedUpdateKey = key;  // save to fill remaining inlist during the cleanup

      // exec or continue to next key
      Status status = Status.UNEXPECTED_STATE;
      if (this.multiUpdateSize > 0) {
        // Commit the batch after it grows beyond the configured size
        if (++numRowsInMultiUpdate % this.multiUpdateSize != 0) {
          return(Status.BATCHED_OK);
        } 
      }
      status = execMultiStmt(this.savedUpdateStatement, this.multiUpdateSize);
      this.savedUpdateStatement=null;
      this.numRowsInMultiUpdate=0;
      return(status);
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }  
  }


  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type, key);
      }
      if (this.ycsbKeyStringType) {
        insertStatement.setString(1, key);
      } else {
        // skip past prefix "user" to make YCSB_KEY data a numeric data
        insertStatement.setInt(1, Integer.parseInt(key.substring(4))); 
      }      
      if (numFields > 0) {
        int index = 2;
        for (String value: fieldInfo.getFieldValues()) {
          insertStatement.setString(index++, value);
        }  
      }
      // Using the batch insert API
      if (batchUpdates) {
        insertStatement.addBatch();
        // Check for a sane batch size
        if (batchSize > 0) {
          // Commit the batch after it grows beyond the configured size
          if (++numRowsInBatch % batchSize == 0) {
            int[] results = insertStatement.executeBatch();
            for (int r : results) {
              // Acceptable values are 1 and SUCCESS_NO_INFO (-2) from reWriteBatchedInserts=true
              if (r != 1 && r != -2) { 
                return Status.ERROR;
              }
            }
            // If autoCommit is off, make sure we commit the batch
            if (!autoCommit) {
              getShardConnectionByKey(key).commit();
            }
            return Status.OK;
          } // else, the default value of -1 or a nonsense. Treat it as an infinitely large batch.
        } // else, we let the batch accumulate
        // Added element to the batch, potentially committing the batch too.
        return Status.BATCHED_OK;
      } else {
        // Normal update
        int result = insertStatement.executeUpdate();
        // If we are not autoCommit, we might have to commit now
        if (!autoCommit) {
          // Let updates be batcher locally
          if (batchSize > 0) {
            if (++numRowsInBatch % batchSize == 0) {
              // Send the batch of updates
              getShardConnectionByKey(key).commit();
            }
            // uhh
            return Status.OK;
          } else {
            // Commit each update
            getShardConnectionByKey(key).commit();
          }
        }
        if (result == 1) {
          return Status.OK;
        }
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      setYcsbKey(deleteStatement, 1, key);

      int result = deleteStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      } else if (result == 0) {
        return Status.NOT_FOUND;
      } else {
        return Status.UNEXPECTED_STATE;
      }
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private OrderedFieldInfo getFieldInfo(Map<String, ByteIterator> values) {
    String fieldKeys = "";
    List<String> fieldValues = new ArrayList<>();
    int count = 0;

    String timeInMicros = "";
    int timeInMillisLen = 0;
    if (this.ycsbPrependTimestamp) {
      // add the trailing space
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS ");
      timeInMicros = LocalDateTime.now().format(formatter).toString();
      timeInMillisLen = timeInMicros.length();
    }

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldKeys += entry.getKey();
      if (count < values.size() - 1) {
        fieldKeys += ",";
      }
      if (this.ycsbPrependTimestamp) {
        fieldValues.add(count, timeInMicros + entry.getValue().toString().substring(timeInMillisLen));
      } else {
        fieldValues.add(count, entry.getValue().toString());
      }
      count++;
    }

    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }
}
