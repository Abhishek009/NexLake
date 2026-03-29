package com.sdf.engine

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, Statement}

import com.sdf.core.dataflow.configparser.{Input, Output, Transform}
import com.sdf.core.dataflow.utils.CommonConfigParser
import org.apache.logging.log4j.LogManager

/**
 * Minimal DuckDB engine implementation compatible with Scala 2.12.
 * - Uses DuckDB JDBC (in-memory) to execute SQL transforms.
 * - Supports file inputs (CSV/Parquet) by using DuckDB's read functions.
 * - Supports jdbc outputs by reading from DuckDB and inserting into target via JDBC.
 */
object DuckDBEngine extends Engine {

  private val logger = LogManager.getLogger(getClass.getSimpleName)

  override def run(pipeline: Any, configVariables: String, usage: String): Unit = {
    val pl = pipeline.asInstanceOf[com.sdf.core.dataflow.configparser.Pipeline]
    // create an in-memory DuckDB connection
    Class.forName("org.duckdb.DuckDBDriver")
    var duckConn: Connection = null
    try {
      duckConn = DriverManager.getConnection("jdbc:duckdb:")
      val jobList = pl.job

      jobList.foreach { j =>
        j match {
          case input: Input => handleInput(input, duckConn, configVariables)
          case transform: Transform => handleTransform(transform, duckConn)
          case output: Output => handleOutput(output, duckConn, configVariables)
          case other => logger.warn(s"Unsupported job element: $other")
        }
      }

    } finally {
      if (duckConn != null) try duckConn.close() catch { case _: Throwable => () }
    }
  }

  private def quoteIdent(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  private def handleInput(input: Input, duckConn: Connection, configVariables: String): Unit = {
    logger.info("DuckDBEngine: creating input table " + input.dfName + " of type " + input.`type`)
    input.`type` match {
      case "file" =>
        val path = input.path.getOrElse("")
        val format = input.format.getOrElse("").toLowerCase
        val cleaned = path.replace("\\", "/")
        val sql = if (format.contains("parquet") || cleaned.toLowerCase.endsWith(".parquet")) {
          "CREATE OR REPLACE VIEW " + quoteIdent(input.dfName) + " AS SELECT * FROM read_parquet('" + cleaned + "')"
        } else {
          // default to CSV auto-detection
          "CREATE OR REPLACE VIEW " + quoteIdent(input.dfName) + " AS SELECT * FROM read_csv_auto('" + cleaned + "')"
        }

        var stmt: Statement = null
        try {
          stmt = duckConn.createStatement()
          logger.info("Executing DuckDB SQL: " + sql)
          stmt.execute(sql)
        } catch {
          case e: Throwable => logger.error("Error creating file input table: " + e.getMessage, e)
        } finally {
          if (stmt != null) try stmt.close() catch { case _: Throwable => () }
        }

      case "jdbc" =>
        val commonConfig = CommonConfigParser.parseConfig(configVariables, "jdbc", input.identifier)
        val sourceUrl = commonConfig.get("url")
        val user = commonConfig.get("username")
        val pass = commonConfig.get("password")
        val tableName = input.table.getOrElse("")
        val schemaName = input.schema.getOrElse("")

        if (sourceUrl == null || tableName == null || tableName.isEmpty) {
          logger.error("JDBC input missing connection or table information")
          return
        }
        val fullTableName = if (schemaName.nonEmpty) schemaName+"."+tableName else tableName

        var srcConn: Connection = null
        var rs: ResultSet = null
        var stmt: Statement = null
        try {
          srcConn = DriverManager.getConnection(sourceUrl, user, pass)
          val s = srcConn.createStatement()
          val selectSql = "SELECT * FROM " + fullTableName
          logger.info("Executing JDBC quert :" + selectSql)
          rs = s.executeQuery(selectSql)

          createDuckDBTableFromResultSet(rs,duckConn,input.dfName)

        } catch {
          case e: Throwable => logger.error("Error copying JDBC input into DuckDB: " + e.getMessage, e)
        } finally {
          if (rs != null) try rs.close() catch { case _: Throwable => () }
          if (stmt != null) try stmt.close() catch { case _: Throwable => () }
          if (srcConn != null) try srcConn.close() catch { case _: Throwable => () }
        }

      case other =>
        logger.warn("Unsupported input type for DuckDBEngine: " + other)
    }
  }


  private def handleTransform(transform: Transform, duckConn: Connection): Unit = {
    logger.info("DuckDBEngine: executing transform " + transform.dfName)
    val sql = transform.query
    var stmt: Statement = null
    try {
      stmt = duckConn.createStatement()
      // Drop the view if it already exists
      val dropViewSql = "DROP VIEW IF EXISTS " + quoteIdent(transform.dfName)
      stmt.execute(dropViewSql)
      val createViewSql = "CREATE OR REPLACE VIEW " + quoteIdent(transform.dfName) + " AS (" + sql + ")"
      logger.info("Executing DuckDB SQL: " + createViewSql)
      stmt.execute(createViewSql)

      if(transform.output != null && transform.output.nonEmpty && transform.output != transform.dfName){
        val dropOutputViewSql = "DROP VIEW IF EXISTS " + quoteIdent(transform.output)
        stmt.execute(dropOutputViewSql)
        val createOutputViewSql = "CREATE OR REPLACE VIEW " + quoteIdent(transform.output) +" AS SELECT * FROM " + quoteIdent(transform.dfName)
        logger.info("Creating output alias view "+ createOutputViewSql)
        stmt.execute(createOutputViewSql)
      }
    } catch {
      case e: Throwable => logger.error("Error executing transform: " + e.getMessage, e)
    } finally {
      if (stmt != null) try stmt.close() catch { case _: Throwable => () }
    }
  }

  private def handleOutput(output: Output, duckConn: Connection, configVariables: String): Unit = {
    logger.info("DuckDBEngine: handling output " + output.dfName + " of type " + output.`type`)
    output.`type` match {
      case "jdbc" =>
        var stmt: Statement = null
        var rs: ResultSet = null
        try {
          stmt = duckConn.createStatement()
          rs = stmt.executeQuery("SELECT * FROM " + quoteIdent(output.dfName))
          val commonConfig = CommonConfigParser.parseConfig(configVariables, "jdbc", output.identifier)

          val targetUrl = commonConfig.get("url")
          logger.info("targetUrl: " + targetUrl)
          val user = commonConfig.get("username")
          logger.info("user: " + user)
          val pass = commonConfig.get("password")
          logger.info("pass: " + pass)
          var tgtConn: Connection = null
          try {
            tgtConn = DriverManager.getConnection(targetUrl, user, pass)
            logger.info("tgtConn: " + tgtConn)
            createTableAndInsertFromResultSet(rs, tgtConn, output.table.getOrElse(output.dfName))
          } catch {
            case e: Throwable => logger.error("Error writing to target JDBC: " + e.getMessage, e)
          } finally {
            if (tgtConn != null) try tgtConn.close() catch { case _: Throwable => () }
          }

        } catch {
          case e: Throwable => logger.error("Error selecting from DuckDB for output: " + e.getMessage, e)
        } finally {
          if (rs != null) try rs.close() catch { case _: Throwable => () }
          if (stmt != null) try stmt.close() catch { case _: Throwable => () }
        }

      case "file" =>
        val path = output.path.getOrElse("")
        val cleaned = path.replace("\\", "/")
        val format = output.outputFormat.getOrElse("parquet").toLowerCase
        logger.info("Output format for file output: "+ format)
        val sql = if (format.contains("csv")) {
          "COPY (SELECT * FROM " + quoteIdent(output.dfName) + ") TO '" + cleaned + "' (HEADER, DELIMITER ',')"
        } else {
          "COPY (SELECT * FROM " + quoteIdent(output.dfName) + ") TO '" + cleaned + "' (FORMAT PARQUET)"
        }
        var stmt2: Statement = null
        try {
          stmt2 = duckConn.createStatement()
          logger.info("Executing DuckDB SQL: " + sql)
          stmt2.execute(sql)
        } catch {
          case e: Throwable => logger.error("Error writing file output from DuckDB: " + e.getMessage, e)
        } finally {
          if (stmt2 != null) try stmt2.close() catch { case _: Throwable => () }
        }

      case other => logger.warn("Unsupported output type for DuckDBEngine: " + other)
    }
  }

  private def createTableAndInsertFromResultSet(rs: ResultSet, targetConn: Connection, targetTable: String): Unit = {
    val md: ResultSetMetaData = rs.getMetaData
    val colCount = md.getColumnCount
    val colDefs = (1 to colCount).map { i =>
      val name = md.getColumnLabel(i)
      val sqlType = md.getColumnTypeName(i).toUpperCase
      val duckType = mapToDuckType(sqlType)
      quoteIdent(name) + " " + duckType
    }.mkString(", ")

    val createSql = "CREATE TABLE IF NOT EXISTS " + quoteIdent(targetTable) + " (" + colDefs + ")"
    logger.info("Creating target table with SQL: " + createSql)
    var stmt: Statement = null
    try {
      stmt = targetConn.createStatement()
      stmt.execute(createSql)
    } catch {
      case e: Throwable => logger.error("Error creating target table: " + e.getMessage, e)
    } finally {
      if (stmt != null) try stmt.close() catch { case _: Throwable => () }
    }

    val placeholders = (1 to colCount).map(_ => "?").mkString(",")
    val cols = (1 to colCount).map(i => quoteIdent(md.getColumnLabel(i))).mkString(",")
    val insertSql = "INSERT INTO " + quoteIdent(targetTable) + " (" + cols + ") VALUES (" + placeholders + ")"

    var pstmt: PreparedStatement = null
    try {
      pstmt = targetConn.prepareStatement(insertSql)
      var rowCount = 0
      while (rs.next()) {
        for (i <- 1 to colCount) pstmt.setObject(i, rs.getObject(i))
        pstmt.addBatch()
        rowCount += 1
        if (rowCount % 500 == 0) pstmt.executeBatch()
      }
      if (rowCount % 500 != 0) pstmt.executeBatch()
      logger.info("Inserted " + rowCount + " rows into " + targetTable)
    } catch {
      case e: Throwable => logger.error("Error inserting rows: " + e.getMessage, e)
    } finally {
      if (pstmt != null) try pstmt.close() catch { case _: Throwable => () }
    }
  }

  private def createDuckDBTableFromResultSet(rs: ResultSet, duckConn: Connection, tableName: String):Unit = {
    val md: ResultSetMetaData = rs.getMetaData
    val colCount = md.getColumnCount

    val colDefs = (1 to colCount).map { i =>
      val name = md.getColumnLabel(i)
      val sqlType = md.getColumnTypeName(i).toUpperCase()
      val duckType = mapToDuckType(sqlType)
      quoteIdent(name) + " " + duckType
    }.mkString(", ")

    // Create table in DuckDB - corrected IF NOT EXISTS and properly quoted identifiers
    val createSql = "CREATE TABLE IF NOT EXISTS " + quoteIdent(tableName) + " (" + colDefs + ")"
    var stmt: Statement = null
    try {
      stmt = duckConn.createStatement()
      logger.info("Creating DuckDB table: " + createSql)
      stmt.execute(createSql)
    } catch {
      case e: Throwable =>
        logger.error("Error creating DuckDB table: " + e.getMessage, e)
        // do not rethrow - log and return so engine can continue
        return
    } finally {
      if (stmt != null) try stmt.close() catch {
        case _: Throwable => ()
      }
    }

    // Insert data from source ResultSet into DuckDB
    val placeHolder = (1 to colCount).map(_ => "?").mkString(",")
    val cols = (1 to colCount).map(i => quoteIdent(md.getColumnLabel(i))).mkString(",")
    val insertSql = "INSERT INTO " + quoteIdent(tableName) + " (" + cols + ") VALUES (" + placeHolder + ")"
    var pstmt: PreparedStatement = null
    try {
      pstmt = duckConn.prepareStatement(insertSql)
      var rowCount = 0
      while (rs.next()) {
        for (i <- 1 to colCount) pstmt.setObject(i, rs.getObject(i))
        pstmt.addBatch()
        rowCount += 1
        if (rowCount % 500 == 0) {
          pstmt.executeBatch()
          logger.info("Inserted " + rowCount + " rows into DuckDB table " + tableName)
        }
      }
      if (rowCount % 500 != 0) {
        pstmt.executeBatch()
        logger.info("Inserted " + rowCount + " rows into DuckDB table " + tableName)

      }
    } catch {
      case e: Throwable =>
        logger.error("Error inserting data into DuckDB table: " + e.getMessage, e)
    } finally {
      if (pstmt != null) try pstmt.close() catch {
        case _: Throwable => ()
      }
    }
  }
  private def mapToDuckType(sqlType: String): String = {
    if (sqlType.contains("INT")) "INTEGER"
    else if (sqlType.contains("CHAR") || sqlType.contains("VARCHAR") || sqlType.contains("TEXT")) "VARCHAR"
    else if (sqlType.contains("DATE") && !sqlType.contains("TIME")) "DATE"
    else if (sqlType.contains("TIMESTAMP") || sqlType.contains("TIME")) "TIMESTAMP"
    else if (sqlType.contains("DOUBLE") || sqlType.contains("FLOAT") || sqlType.contains("NUMERIC") || sqlType.contains("DECIMAL")) "DOUBLE"
    else "VARCHAR"
  }

}
