#!/bin/bash

# ===========================
# Airflow Setup Script
# ===========================

# Stop on any error
set -e

# 1️⃣ Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv

# 2️⃣ Activate venv
echo "Activating virtual environment..."
source venv/bin/activate

# 3️⃣ Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip setuptools wheel

# 4️⃣ Install Airflow with constraints
AIRFLOW_VERSION=2.7.1
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing Apache Airflow ${AIRFLOW_VERSION}..."
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 5️⃣ Set Airflow home and SQLite backend
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
mkdir -p $AIRFLOW_HOME

echo "Airflow home set to: $AIRFLOW_HOME"
echo "Using SQLite as backend..."

# 6️⃣ Initialize Airflow DB
echo "Initializing Airflow DB..."
airflow db init

# 7️⃣ Create Admin User
echo "Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Sunbeam \
    --lastname Admin \
    --role Admin \
    --email nehamorkhande1707@gmail.com \
    --password admin

# 8️⃣ Launch Airflow Webserver (background)
echo "Starting Airflow webserver on port 8080..."
nohup airflow webserver --port 8080 > airflow_webserver.log 2>&1 &

# 9️⃣ Launch Airflow Scheduler (background)
echo "Starting Airflow scheduler..."
nohup airflow scheduler > airflow_scheduler.log 2>&1 &

# 1️⃣0️⃣ Done
echo "✅ Airflow setup complete!"
echo "Webserver: http://localhost:8080 (Username: admin / Password: admin)"
echo "Logs: airflow_webserver.log, airflow_scheduler.log"
