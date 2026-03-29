#!/usr/bin/env bash
# =============================================================================
#  SparkDataFlow — Job Runner
#  Usage:
#    ./run_job.sh --engine <spark|duckdb> \
#                 --jobFile <job.yml> \
#                 --jobConfig <config.yml> \
#                 --configFile <meta_config.json> \
#                [--jar <path/to/sparkdataflow.jar>] \
#                [--skip-validation] \
#                [--dry-run]
# =============================================================================

set -euo pipefail

# ─────────────────────────────────────────────
#  Colours
# ─────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# ─────────────────────────────────────────────
#  Logging helpers
# ─────────────────────────────────────────────
log_info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
log_success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
log_section() { echo -e "\n${BOLD}── $* ──────────────────────────────────────${RESET}"; }

# ─────────────────────────────────────────────
#  Defaults
# ─────────────────────────────────────────────
ENGINE=""
JOB_FILE=""
JOB_CONFIG=""
CONFIG_FILE=""
JAR_PATH=""
SKIP_VALIDATION=false
DRY_RUN=false

# Default JAR location (override with --jar)
DEFAULT_JAR="$(dirname "$0")/target/sparkdataflow-1.0.jar"

# Python validator script (expected alongside this script)
VALIDATOR_SCRIPT="$(dirname "$0")/yaml_validator.py"

# Main entrypoint class
MAIN_CLASS="com.sdf.core.dataflow.Flow"

# ─────────────────────────────────────────────
#  DuckDB fallback default for heap size
#  (used only if duckdb.memory_limit is absent
#  from jobConfig)
# ─────────────────────────────────────────────
DUCKDB_MEMORY="${DUCKDB_MEMORY:-4g}"

# ─────────────────────────────────────────────
#  Parse arguments
# ─────────────────────────────────────────────
usage() {
  echo ""
  echo -e "${BOLD}Usage:${RESET}"
  echo "  ./run_job.sh --engine <spark|duckdb>"
  echo "               --jobFile    <path/to/job.yml>"
  echo "               --jobConfig  <path/to/config.yml>"
  echo "               --configFile <path/to/meta_config.json>"
  echo "              [--jar        <path/to/sparkdataflow.jar>]"
  echo "              [--skip-validation]"
  echo "              [--dry-run]"
  echo ""
  echo -e "${BOLD}Engines:${RESET}"
  echo "  spark   — Runs via spark-submit (requires SPARK_HOME)"
  echo "  duckdb  — Runs via java -cp (requires JAVA_HOME)"
  echo ""
  echo -e "${BOLD}Environment variables (Spark):${RESET}"
  echo "  SPARK_HOME  — required, path to your Spark installation"
  echo ""
  echo -e "${BOLD}Environment variables (DuckDB):${RESET}"
  echo "  JAVA_HOME   — optional, defaults to java on PATH"
  echo "  DUCKDB_MEMORY — fallback heap size if duckdb.memory_limit not in jobConfig (default: 4g)"
  echo ""
  exit 1
}

if [[ $# -eq 0 ]]; then usage; fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine)          ENGINE="$2";          shift 2 ;;
    --jobFile)         JOB_FILE="$2";        shift 2 ;;
    --jobConfig)       JOB_CONFIG="$2";      shift 2 ;;
    --configFile)      CONFIG_FILE="$2";     shift 2 ;;
    --jar)             JAR_PATH="$2";        shift 2 ;;
    --skip-validation) SKIP_VALIDATION=true; shift   ;;
    --dry-run)         DRY_RUN=true;         shift   ;;
    -h|--help)         usage ;;
    *) log_error "Unknown argument: $1"; usage ;;
  esac
done

# ─────────────────────────────────────────────
#  Resolve JAR path
# ─────────────────────────────────────────────
JAR_PATH="${JAR_PATH:-$DEFAULT_JAR}"

# ─────────────────────────────────────────────
#  Banner
# ─────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║       SparkDataFlow Job Runner       ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════╝${RESET}"
echo ""

# ─────────────────────────────────────────────
#  Step 1 — Validate required arguments
# ─────────────────────────────────────────────
log_section "Step 1: Checking arguments"

MISSING=false

[[ -z "$ENGINE"      ]] && { log_error "--engine is required (spark|duckdb)";   MISSING=true; }
[[ -z "$JOB_FILE"    ]] && { log_error "--jobFile is required";                  MISSING=true; }
[[ -z "$JOB_CONFIG"  ]] && { log_error "--jobConfig is required";                MISSING=true; }
[[ -z "$CONFIG_FILE" ]] && { log_error "--configFile is required";               MISSING=true; }
[[ "$MISSING" == true ]] && usage

ENGINE_LOWER=$(echo "$ENGINE" | tr '[:upper:]' '[:lower:]')
if [[ "$ENGINE_LOWER" != "spark" && "$ENGINE_LOWER" != "duckdb" ]]; then
  log_error "Invalid engine '${ENGINE}'. Must be 'spark' or 'duckdb'."
  exit 1
fi

log_success "Engine      : ${BOLD}${ENGINE_LOWER}${RESET}"
log_info    "Job file    : $JOB_FILE"
log_info    "Job config  : $JOB_CONFIG"
log_info    "Meta config : $CONFIG_FILE"
log_info    "JAR         : $JAR_PATH"
[[ "$DRY_RUN" == true ]]         && log_warn "Mode        : DRY RUN (no job will be executed)"
[[ "$SKIP_VALIDATION" == true ]] && log_warn "Validation  : SKIPPED"

# ─────────────────────────────────────────────
#  Step 2 — Check required files exist
# ─────────────────────────────────────────────
log_section "Step 2: Checking files"

check_file() {
  local label="$1" path="$2"
  if [[ ! -f "$path" ]]; then
    log_error "$label not found: '$path'"
    exit 1
  fi
  log_success "$label found: $path"
}

check_file "Job YAML"     "$JOB_FILE"
check_file "Job config"   "$JOB_CONFIG"
check_file "Meta config"  "$CONFIG_FILE"
check_file "JAR"          "$JAR_PATH"

# ─────────────────────────────────────────────
#  Step 3 — YAML validation
# ─────────────────────────────────────────────
log_section "Step 3: YAML validation"

if [[ "$SKIP_VALIDATION" == true ]]; then
  log_warn "Skipping YAML validation (--skip-validation flag set)."
else
  if [[ ! -f "$VALIDATOR_SCRIPT" ]]; then
    log_warn "Validator script not found at '$VALIDATOR_SCRIPT' — skipping validation."
    log_warn "Place yaml_validator.py alongside run_job.sh to enable validation."
  else
    # Check Python is available
    if ! command -v python3 &>/dev/null && ! command -v python &>/dev/null; then
      log_warn "Python not found — skipping YAML validation."
    else
      PYTHON_CMD=$(command -v python3 || command -v python)
      log_info "Running YAML validation with $PYTHON_CMD ..."
      echo ""

      if ! "$PYTHON_CMD" "$VALIDATOR_SCRIPT" "$JOB_FILE"; then
        echo ""
        log_error "YAML validation failed. Fix the errors above before re-running."
        exit 1
      fi
      log_success "YAML validation passed."
    fi
  fi
fi

# ─────────────────────────────────────────────
#  Step 4 — Engine pre-flight checks
# ─────────────────────────────────────────────
log_section "Step 4: Engine pre-flight"

# ─────────────────────────────────────────────
#  jobConfig parser
#  Reads key=value lines, skips blanks/comments
#  Returns value for a given key, or a default
# ─────────────────────────────────────────────

# Read jobConfig into two parallel arrays
CONFIG_KEYS=()
CONFIG_VALS=()

parse_job_config() {
  local file="$1"
  log_info "Parsing jobConfig: $file"
  while IFS='=' read -r raw_key raw_val; do
    # Skip blank lines and comments
    [[ -z "$raw_key" || "$raw_key" =~ ^[[:space:]]*# ]] && continue
    # Trim whitespace
    local key val
    key=$(echo "$raw_key" | xargs)
    val=$(echo "$raw_val" | xargs)
    [[ -z "$key" ]] && continue
    CONFIG_KEYS+=("$key")
    CONFIG_VALS+=("$val")
    log_info "  loaded: ${key} = ${val}"
  done < "$file"
  echo ""
}

# Look up a key in the parsed config; echo the value or the default
config_get() {
  local key="$1" default="$2"
  for i in "${!CONFIG_KEYS[@]}"; do
    if [[ "${CONFIG_KEYS[$i]}" == "$key" ]]; then
      echo "${CONFIG_VALS[$i]}"
      return
    fi
  done
  echo "$default"
}

# ─────────────────────────────────────────────
#  Spark — every key=value from jobConfig
#  is passed as --conf key=value directly.
#  No mapping needed — Spark handles all
#  spark.* properties natively.
# ─────────────────────────────────────────────

run_spark() {
  if [[ -z "${SPARK_HOME:-}" ]]; then
    log_error "SPARK_HOME is not set. Please export SPARK_HOME=/path/to/spark"
    exit 1
  fi
  if [[ ! -f "$SPARK_HOME/bin/spark-submit" ]]; then
    log_error "spark-submit not found at '$SPARK_HOME/bin/spark-submit'"
    exit 1
  fi
  log_success "SPARK_HOME : $SPARK_HOME"

  # Parse jobConfig
  parse_job_config "$JOB_CONFIG"

  CMD=(
    "$SPARK_HOME/bin/spark-submit"
    --class "$MAIN_CLASS"
  )

  # Every key in the config file becomes --conf key=value
  for i in "${!CONFIG_KEYS[@]}"; do
    local key="${CONFIG_KEYS[$i]}"
    local val="${CONFIG_VALS[$i]}"
    CMD+=("--conf" "${key}=${val}")
    log_info "  → --conf ${key}=${val}"
  done

  CMD+=(
    "$JAR_PATH"
    --jobFile    "$JOB_FILE"
    --jobConfig  "$JOB_CONFIG"
    --configFile "$CONFIG_FILE"
  )
}

# ─────────────────────────────────────────────
#  DuckDB — all keys become -Dkey=value JVM
#  system properties, except:
#    duckdb.memory_limit → -Xmx (JVM heap)
# ─────────────────────────────────────────────

run_duckdb() {
  if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
  elif command -v java &>/dev/null; then
    JAVA_CMD="java"
  else
    log_error "Java not found. Set JAVA_HOME or ensure java is on your PATH."
    exit 1
  fi

  JAVA_VERSION=$("$JAVA_CMD" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  log_success "Java        : $JAVA_CMD ($JAVA_VERSION)"

  # Parse jobConfig
  parse_job_config "$JOB_CONFIG"

  # duckdb.memory_limit maps to -Xmx; fall back to env/default
  local heap
  heap=$(config_get "duckdb.memory_limit" "$DUCKDB_MEMORY")
  log_info "JVM heap (-Xmx) : $heap"

  CMD=(
    "$JAVA_CMD"
    "-Xmx${heap}"
  )

  # All other duckdb.* keys → -Dkey=value
  for i in "${!CONFIG_KEYS[@]}"; do
    local key="${CONFIG_KEYS[$i]}"
    local val="${CONFIG_VALS[$i]}"

    # memory_limit already handled as -Xmx
    [[ "$key" == "duckdb.memory_limit" ]] && continue

    CMD+=("-D${key}=${val}")
    log_info "  → -D${key}=${val}"
  done

  CMD+=(
    -cp "$JAR_PATH"
    "$MAIN_CLASS"
    --jobFile    "$JOB_FILE"
    --jobConfig  "$JOB_CONFIG"
    --configFile "$CONFIG_FILE"
  )
}

case "$ENGINE_LOWER" in
  spark)  run_spark  ;;
  duckdb) run_duckdb ;;
esac

# ─────────────────────────────────────────────
#  Step 5 — Execute
# ─────────────────────────────────────────────
log_section "Step 5: Executing job"

# Pretty-print the command
echo -e "${CYAN}Command:${RESET}"
echo "  ${CMD[*]}"
echo ""

if [[ "$DRY_RUN" == true ]]; then
  log_warn "DRY RUN — command printed above but not executed."
  exit 0
fi

START_TIME=$(date +%s)
log_info "Job started at $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Run and capture exit code without letting set -e kill us
set +e
"${CMD[@]}"
EXIT_CODE=$?
set -e

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
log_section "Result"
if [[ $EXIT_CODE -eq 0 ]]; then
  log_success "Job completed successfully in ${MINUTES}m ${SECONDS}s ✅"
else
  log_error   "Job FAILED with exit code $EXIT_CODE after ${MINUTES}m ${SECONDS}s ❌"
  exit $EXIT_CODE
fi