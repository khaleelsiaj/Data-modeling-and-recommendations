#!/bin/bash
LOG_DIR="logs/bash_logs"
LOG_FILE="$LOG_DIR/pipeline_run_$(date +%Y%m%d_%H%M%S).log"
PYTHON="python3"

mkdir -p $LOG_DIR

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')]: $1" | tee -a $LOG_FILE
}

log "Starting pipeline execution"

#check and activate the virtual environment
if [ -f "venv/bin/activate" ]; then
    source "venv/bin/activate"
    log "virtual environment activated"
else
    log "ERROR: virtual environment not found"
    exit 1

fi

# check and load credintials
if [ -f ".env" ]; then
    export $(cat .env | xargs)
    log "credintials imported"
else
    log "ERROR: .env file not found"
    exit 1
fi

# run db_setup.py module
log "Running db_setup.py"
$PYTHON db_setup.py >/dev/null 2>&1

if [ $? -eq 0 ]; then
    log "db_setup ran successfully"
else
    log "ERROR: db_setup failed"
    deactivate
    exit 1

fi

# run etl_pipeline module
log "Running etl_pipeline.py"

$PYTHON etl_pipeline.py >/dev/null 2>&1

if [ $? -eq 0 ]; then
    log "etl_pipeline ran successfully"

else
    log "etl_pipeline failed"
    deactivate
    exit 1
fi

deactivate
log "Pipeline excution finished"
