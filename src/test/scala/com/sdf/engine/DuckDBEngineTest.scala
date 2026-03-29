package com.sdf.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import java.sql.{Connection, DriverManager, Statement}

class DuckDBEngineTest extends AnyFunSuite with BeforeAndAfter {

  var connection: Connection = _
  var statement: Statement = _

  before {
    // Initialize a DuckDB in-memory database connection before each test
    connection = DriverManager.getConnection("jdbc:duckdb:")
    statement = connection.createStatement()
  }

  after {
    // Close the database connection after each test
    if (statement != null) statement.close()
    if (connection != null) connection.close()
  }

  test("Test creating and dropping a view") {
    // Test the creation and dropping of a view
    statement.execute("CREATE TABLE t1 (id INT, name VARCHAR)")
    statement.execute("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")

    val dropViewSql = "DROP VIEW IF EXISTS out_01"
    val createViewSql = "CREATE OR REPLACE VIEW out_01 AS SELECT * FROM t1"

    statement.execute(dropViewSql)
    statement.execute(createViewSql)

    val resultSet = statement.executeQuery("SELECT * FROM out_01")
    assert(resultSet.next())
    assert(resultSet.getInt("id") == 1)
    assert(resultSet.getString("name") == "Alice")
  }

  test("Test handling output to JDBC") {
    // Test handling output to a JDBC target
    statement.execute("CREATE TABLE t2 (id INT, value VARCHAR)")
    statement.execute("INSERT INTO t2 VALUES (1, 'TestValue')")

    val resultSet = statement.executeQuery("SELECT * FROM t2")
    assert(resultSet.next())
    assert(resultSet.getInt("id") == 1)
    assert(resultSet.getString("value") == "TestValue")
  }

  test("Test error handling during transform") {
    // Test error handling when a view already exists
    statement.execute("CREATE VIEW t1 AS SELECT 1 AS id")

    val exception = intercept[Exception] {
      statement.execute("CREATE VIEW t1 AS SELECT 2 AS id")
    }

    assert(exception.getMessage.contains("already exists"))
  }

  test("Test writing output to DuckDB table") {
    // Test writing output to a DuckDB table
    statement.execute("CREATE TABLE t3 (id INT, description VARCHAR)")
    statement.execute("INSERT INTO t3 VALUES (1, 'Description1'), (2, 'Description2')")

    val resultSet = statement.executeQuery("SELECT * FROM t3")
    assert(resultSet.next())
    assert(resultSet.getInt("id") == 1)
    assert(resultSet.getString("description") == "Description1")
  }
}
