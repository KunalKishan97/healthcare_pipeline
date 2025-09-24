import os
import pandas as pd
import duckdb
from datetime import datetime


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'medallion.duckdb')


con = duckdb.connect(DB_PATH)



con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
con.execute("CREATE SCHEMA IF NOT EXISTS curated;")
con.execute("CREATE SCHEMA IF NOT EXISTS analytic;")

ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


sources = {
    'patients': {
        'file': os.path.join(DATA_DIR, 'Patient.csv'),
        'columns': {
            'PatientID': 'INTEGER',
            'firstname': 'VARCHAR',
            'lastname':'VARCHAR',
            'email': 'VARCHAR',
            'ingestion_date': 'TIMESTAMP'
        },
        'pk': 'PatientID'
    },
    'doctors': {
        'file': os.path.join(DATA_DIR, 'Doctor.csv'),
        'columns': {
            'DoctorID': 'INTEGER',
            'DoctorName': 'VARCHAR',
            'Specialization': 'VARCHAR',
            'ingestion_date': 'TIMESTAMP'
        },
        'pk': 'DoctorID'
    },
    'appointments': {
        'file': os.path.join(DATA_DIR, 'Appointment.csv'),
        'columns': {
            'AppointmentID': 'INTEGER',
            'PatientID': 'INTEGER',
            'DoctorID': 'INTEGER',
            'Date':'TIMESTAMP',
            'ingestion_date': 'TIMESTAMP'
        },
        'pk': 'AppointmentID'
    },
    'medicalprocedure': {
        'file': os.path.join(DATA_DIR, 'Medical Procedure.csv'),
        'columns': {
            'AppointmentID': 'INTEGER',
            'ProcedureID': 'INTEGER',
            'ProcedureName': 'VARCHAR',
            'ingestion_date': 'TIMESTAMP'
        },
        'pk': 'AppointmentID,ProcedureID'
    }
}

for table_name, meta in sources.items():
    # Read CSV
    df = pd.read_csv(meta['file'])
    df.columns = df.columns.str.strip() 
    df['ingestion_date'] = ingestion_date

    print(f"Processing table: {table_name}")
    print("CSV columns after strip:", df.columns.tolist())


    for col, dtype in meta['columns'].items():
        if col not in df.columns and col != 'ingestion_date':
            print(f"Warning: Column '{col}' not found in CSV for table {table_name}, skipping cast")
            continue
        if dtype == 'INTEGER':
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif dtype == 'TIMESTAMP':
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:  # VARCHAR
            df[col] = df[col].astype(str)

   
    columns_def = ', '.join([f"{col} {dtype}" for col, dtype in meta['columns'].items()])
    con.execute(f"CREATE TABLE IF NOT EXISTS raw.{table_name} ({columns_def})")


    con.execute(f"ALTER TABLE raw.{table_name} ADD COLUMN IF NOT EXISTS ingestion_date TIMESTAMP")

   
    con.register('tmp_df', df)


    pk_list = [k.strip() for k in meta['pk'].split(',')]
    where_clause = " AND ".join([f"r.{k} = tmp_df.{k}" for k in pk_list])

    cast_columns = []
    for c in meta['columns'].keys():
        cast_columns.append(f"CAST(tmp_df.{c} AS {meta['columns'][c]}) AS {c}")
    cast_columns_sql = ', '.join(cast_columns)

    
    con.execute(f"""
        INSERT INTO raw.{table_name} ({', '.join(meta['columns'].keys())})
        SELECT {cast_columns_sql}
        FROM tmp_df
        WHERE NOT EXISTS (
            SELECT 1 FROM raw.{table_name} r
            WHERE {where_clause}
        )
    """)


    con.unregister('tmp_df')

    print(f"Table raw.{table_name} ingested successfully.\n")

print("\nAll raw tables ingested successfully. DuckDB file at:", DB_PATH)

