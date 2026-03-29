package com.sdf.engine

import org.scalatest.funsuite.AnyFunSuite
import java.io.File
import scala.sys.process._

class DuckDBEngineIntegrationTest extends AnyFunSuite {

  // Helper method to run the engine with a given YAML config
  def runEngineWithConfig(configPath: String): Int = {
    val isWindows = System.getProperty("os.name").toLowerCase.contains("win")
    val jobConfig = "example/conf/config.yml"
    val metaConfig = "example/conf/meta.conf"
    if (isWindows) {
      val cmd = Seq("example/src/run_sdf.bat", "--engine", "duckdb", "--jobFile", configPath, "--jobConfig", jobConfig, "--configFile", metaConfig)
      Process(cmd).!
    } else {
      val cmd = Seq("sh", "example/src/run_sdf.sh", "--engine", "duckdb", "--jobFile", configPath, "--jobConfig", jobConfig, "--configFile", metaConfig)
      Process(cmd).!
    }
  }

  /**
   * Test: multi_output_test.yml
   * This test runs the engine with the multi_output_test.yml config,
   * which reads a Parquet file and writes to two output Parquet files using DuckDB.
   * It verifies that both output files are created.
   */
  test("DuckDB multi output test: should create both output files") {
    val configPath = "example/yaml/duckdb/multi_output_test.yml"
    val out1 = new File("data/out1.parquet")
    val out2 = new File("data/out2.parquet")

    // Clean up before test
    if (out1.exists) out1.delete()
    if (out2.exists) out2.delete()

    val exitCode = runEngineWithConfig(configPath)
    assert(exitCode == 0, "Engine did not exit cleanly")
    assert(out1.exists, "Output file out1.parquet was not created")
    assert(out2.exists, "Output file out2.parquet was not created")
  }

  /**
   * Test: duckdb_to_file.yml
   * This test reads from a DuckDB table and writes to a file.
   * It verifies that the output file is created.
   */
  test("DuckDB table to file: should create output file") {
    val configPath = "example/yaml/duckdb/duckdb_to_file.yml"
    val outFile = new File("data/duckdb_to_file_out.parquet")
    if (outFile.exists) outFile.delete()
    val exitCode = runEngineWithConfig(configPath)
    assert(exitCode == 0, "Engine did not exit cleanly")
    assert(outFile.exists, "Output file was not created")
  }

  /**
   * Test: file_to_duckdb_table.yml
   * This test reads from a file and writes to a DuckDB table.
   * It verifies that the table exists in the DuckDB database after execution.
   */
  test("File to DuckDB table: should create output table") {
    val configPath = "example/yaml/duckdb/file_to_duckdb_table.yml"
    val dbFile = new File("data/sdf.duckdb")
    if (dbFile.exists) dbFile.delete()
    val exitCode = runEngineWithConfig(configPath)
    assert(exitCode == 0, "Engine did not exit cleanly")
    // Check table existence using DuckDB CLI
    val checkTableCmd = s"""duckdb ${dbFile.getPath} "SELECT COUNT(*) FROM output_table;""""
    val tableCheck = checkTableCmd.!
    assert(tableCheck == 0, "Output table does not exist in DuckDB database")
  }

  /**
   * Test: join_test.yml
   * This test performs a join between two inputs and writes the result to a file.
   * It verifies that the output file is created.
   */
  test("DuckDB join test: should create joined output file") {
    val configPath = "example/yaml/duckdb/join_test.yml"
    val outFile = new File("data/joined.parquet")
    if (outFile.exists) outFile.delete()
    val exitCode = runEngineWithConfig(configPath)
    assert(exitCode == 0, "Engine did not exit cleanly")
    assert(outFile.exists, "Joined output file was not created")
  }

  /**
   * Test: aggregation_test.yml
   * This test performs aggregation and writes the result to a file.
   * It verifies that the output file is created.
   */
  test("DuckDB aggregation test: should create aggregated output file") {
    val configPath = "example/yaml/duckdb/aggregation_test.yml"
    val outFile = new File("data/aggregation_out.parquet")
    if (outFile.exists) outFile.delete()
    val exitCode = runEngineWithConfig(configPath)
    assert(exitCode == 0, "Engine did not exit cleanly")
    assert(outFile.exists, "Aggregated output file was not created")
  }

  /**
   * Test: error_test.yml
   * This test checks error handling for a non-existent table.
   * It expects the engine to fail gracefully.
   */
  test("DuckDB error test: should fail gracefully on missing table") {
    val configPath = "example/yaml/duckdb/error_test.yml"
    val exitCode = runEngineWithConfig(configPath)
    assert(exitCode != 0, "Engine should fail for missing table, but exited cleanly")
  }

  /**
   * Test: multi_step_pipeline.yml
   * This test runs a multi-step pipeline and verifies the final output file is created.
   */
  test("DuckDB multi-step pipeline: should create final output file") {
    val configPath = "example/yaml/duckdb/multi_step_pipeline.yml"
    val outFile = new File("data/multi_step_out.parquet")
    if (outFile.exists) outFile.delete()
    val exitCode = runEngineWithConfig(configPath)
    assert(exitCode == 0, "Engine did not exit cleanly")
    assert(outFile.exists, "Multi-step pipeline output file was not created")
  }

  // Add more tests for other YAML configs as needed, following the same pattern
}
