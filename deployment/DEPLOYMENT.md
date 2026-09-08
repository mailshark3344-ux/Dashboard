CDC Dashboard Deployment
1. Prerequisites

The target system must have:

Docker
Docker Compose
Existing MinIO
Network access from the Docker containers to MinIO

The target system must also provide the required MinIO endpoint and credentials.

2. Load Application Images

Copy:

cdc-dashboard-images.tar


to the target Docker machine.

Run:

docker load -i cdc-dashboard-images.tar


Verify:

docker images


The following custom images should be available:

cdc-dashboard-python-worker
cdc-dashboard-file-picker

3. Configure Environment

Copy:

.env.example


to:

.env


Fill in the target system's values for:

PostgreSQL credentials
Redash URL
MinIO endpoint
MinIO credentials
Redash secrets
CDC configuration

Example:

REDASH_URL=http://target-redash-host:5000
REDASH_DASHBOARD_PATH=/dashboards/1-dashboard


REDASH_URL should contain the Redash base URL only.

The dashboard path is configured separately using:

REDASH_DASHBOARD_PATH=/dashboards/1-dashboard


Do not use the development machine's passwords or MinIO credentials unless specifically required.

4. PostgreSQL / Redash Database

The deployment package contains:

redash-backup.sql


This is an optional PostgreSQL/Redash migration backup.

It contains the existing Redash configuration/data and CDC-related data from the development environment.

There are two deployment options.

Option A — Migrate Existing Data

Choose this option if the target team wants to keep the existing:

Redash dashboards
Redash queries
Redash configuration
CDC data
cdc_events records
processed_files history
Other data contained in the PostgreSQL backup
Important

The backup must be restored into a new/empty PostgreSQL database.

Do not restore redash-backup.sql on top of an already initialized Redash database containing the same tables.

Otherwise errors such as:

relation already exists
duplicate key value violates unique constraint
multiple primary keys are not allowed
constraint already exists


may occur.

After a successful restore, configure the deployment to use the migrated PostgreSQL database.

The target MinIO must also contain/provide access to the required CDC files if the existing CDC workflow is expected to continue processing them.

Option B — Fresh Deployment

Choose this option if the target team does not need the existing Redash/CDC data.

In this case:

Do not restore redash-backup.sql.

Start with a new/empty PostgreSQL database.

The Redash database schema will be initialized by the deployment.

The CDC application can then populate the required CDC tables as CDC files are processed.

The initial CDC data will be empty until files are processed from the target MinIO.

5. Start the Services

After .env is configured:

docker compose up -d


Check:

docker compose ps


All required services should be running.

6. Verify PostgreSQL

Check that PostgreSQL is healthy:

docker compose ps postgres


If the CDC table exists, check the record count:

docker exec cdc-postgres psql -U redash -d redash -c "SELECT COUNT(*) FROM cdc_events;"


For a fresh deployment, the CDC data may initially be empty.

For an existing-data migration, the expected records should be present after a successful restore.

7. Verify CDC Worker

Check the worker:

docker logs --tail 50 cdc-python-worker


The worker should be able to connect to:

Target MinIO
Target PostgreSQL


A successfully processed file should result in CDC records being stored in PostgreSQL.

Already processed files may be skipped using the processed_files tracking information.

8. Verify File Picker

The File Picker is exposed on port 7000.

Open:

http://localhost:7000/select


Select a CDC file.

The File Picker should redirect the browser to the configured Redash dashboard.

The Redash URL is controlled by:

REDASH_URL
REDASH_DASHBOARD_PATH

9. Verify Redash

Open the configured Redash URL and confirm that:

Redash loads successfully
The dashboard is accessible
CDC data is visible
Queries execute successfully
10. Expected Data Flow

The expected application flow is:

MinIO
  ↓
Python CDC Worker
  ↓
PostgreSQL
  ↓
cdc_events
  ↓
Redash
  ↓
Dashboard


The File Picker provides the browser entry point:

Browser
  ↓
File Picker :7000
  ↓
Redash Dashboard

11. Deployment Package

The deployment package contains:

deployment/
│
├── cdc-dashboard-images.tar
├── docker-compose.yml
├── .env.example
├── DEPLOYMENT.md
└── redash-backup.sql


The .env file contains environment-specific credentials and should not be shared unless specifically required.

redash-backup.sql is optional and should only be used when the target team chooses Option A — Migrate Existing Data.

12. Important Migration Rule

Do not automatically restore redash-backup.sql.

The target team must first decide:

Existing data required?
        │
        ├── YES → Restore backup into a new/empty PostgreSQL database
        │
        └── NO  → Skip backup and perform a fresh deployment


Both deployment options are supported by this package.