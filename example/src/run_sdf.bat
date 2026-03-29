@echo off
REM ============================================================================
REM  SparkDataFlow — Job Runner (Windows Batch)
REM  Usage:
REM    run_sdf.bat --engine duckdb --jobFile <job.yml> --jobConfig <config.yml> --configFile <meta.conf> [--jar <jar>]
REM ============================================================================

setlocal enabledelayedexpansion

REM ---- Defaults ----
set ENGINE=
set JOB_FILE=
set JOB_CONFIG=
set CONFIG_FILE=
set JAR_PATH=
set DEFAULT_JAR=%~dp0..\..\target\SparkDataFlow.jar
set DUCKDB_MEMORY=4g

REM ---- Parse arguments ----
:parse_args
if "%~1"=="" goto end_parse
if "%~1"=="--engine"        set ENGINE=%~2 & shift & shift & goto parse_args
if "%~1"=="--jobFile"       set JOB_FILE=%~2 & shift & shift & goto parse_args
if "%~1"=="--jobConfig"     set JOB_CONFIG=%~2 & shift & shift & goto parse_args
if "%~1"=="--configFile"    set CONFIG_FILE=%~2 & shift & shift & goto parse_args
if "%~1"=="--jar"           set JAR_PATH=%~2 & shift & shift & goto parse_args
shift & goto parse_args
:end_parse

if "%JAR_PATH%"=="" set JAR_PATH=%DEFAULT_JAR%

REM ---- Only support DuckDB for now ----
if /I not "%ENGINE%"=="duckdb" (
  echo Only --engine duckdb is supported in this batch script.
  exit /b 1
)

REM ---- Parse jobConfig for duckdb.memory_limit ----
set HEAP=%DUCKDB_MEMORY%
for /f "usebackq tokens=1,2 delims==" %%A in ("%JOB_CONFIG%") do (
  set key=%%A
  set val=%%B
  if /I "!key!"=="duckdb.memory_limit" set HEAP=!val!
)

REM ---- Build -D properties from jobConfig ----
set DPROPS=
for /f "usebackq tokens=1,2 delims==" %%A in ("%JOB_CONFIG%") do (
  set key=%%A
  set val=%%B
  if /I not "!key!"=="duckdb.memory_limit" set DPROPS=!DPROPS! -D!key!=!val!
)

REM ---- Run the job ----
set CMD=java -Xmx%HEAP% %DPROPS% -cp "%JAR_PATH%" com.sdf.core.dataflow.Flow --jobFile "%JOB_FILE%" --jobConfig "%JOB_CONFIG%" --configFile "%CONFIG_FILE%"
echo Running: %CMD%
%CMD%

endlocal

