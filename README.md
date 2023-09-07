# General Tool for Moving Data Between a Database and S3

How many times have you struggled to export data and store in someplace for safekeeping. This tool aims to help solve this problem.

## Getting Started
1. Clone the repository:

```bash
git clone https://github.com/jamesvgibbs/export_rent_manager.git
```

2. Change to the project directory:

```bash
cd your-repository
```

3. Setup Env Variables

```bash
cp .env.example .env
```

```
RM_API_URL= Rent Manager API URL 
RM_API_LOCATION_ID= Rent Manager Location ID
RM_API_PASSWORD= Rent Manager API User Password
RM_API_USERNAME= Rent Manager API Username

API_TOKEN= This is filled with the RM API Token

RM_DB_DATABASE= Rent Manager Remote Database
RM_DB_HOST= Rent Manager Remote Host
RM_DB_PASSWORD= Rent Manager DB User Password
RM_DB_USER= Rent Manager DB Username 

WH_DB_HOST= Your Database Server Host
WH_DB_USER= Your Database Server Username
WH_DB_PASSWORD= Your Database Server User Password
WH_DB_DATABASE= Your Database Name
WH_DB_PORT=5432
```

4. Run the code

```bash
python greatcontrol.py
```