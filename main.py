import os
import datetime
import logging
import pyodbc
import dotenv
from openpyxl import Workbook

# Import parser modules
from Parser.CPI import main as cpi_main
from Parser.EUFATF import main as eufatf_main
from Parser.EUsanctions import main as eusanctions_main
from Parser.EUtax import main as eutax_main
from Parser.FATF_CFA import main as fatfcfa_main
from Parser.FATF_IM import main as fatfim_main
from Parser.FRsanctions import main as frsanctions_main
from Parser.FRtax import main as frtax_main
from Parser.OFAC import main as ofac_main
from Parser.UKsanctions import main as uksanctions_main

# Load environment variables from .env file
dotenv.load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch data from a table in the database
def fetch_table_data(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    return rows, columns

# Function to log changes to the audit table
def log_changes_to_audit_table(cursor, old_rows, new_rows, columns):
    try:
        changes_detected = False

        for old_row, new_row in zip(old_rows, new_rows):
            country_id = old_row[columns.index('SanctionsMapId')]

            for i, column in enumerate(columns):
                old_value = old_row[i]
                new_value = new_row[i]

                if old_value != new_value:
                    changes_detected = True
                    audit_sql = """
                        INSERT INTO TblSanctionsMap_Audit (
                            SanctionsMapId, ColumnName, OldValue, NewValue, UpdatedAt
                        ) VALUES (?, ?, ?, ?, ?)
                    """
                    cursor.execute(audit_sql, country_id, column, old_value, new_value, datetime.datetime.now())
                    logging.info(f"Logged change for ID {country_id} in column {column}: {old_value} -> {new_value}")

        if changes_detected:
            cursor.connection.commit()
        else:
            audit_sql = """
                INSERT INTO TblSanctionsMap_Audit (
                    SanctionsMapId, ColumnName, OldValue, NewValue, UpdatedAt
                ) VALUES (?, ?, ?, ?, ?)
            """
            cursor.execute(audit_sql, -1, 'None', 'No changes detected', 'No changes detected', datetime.datetime.now())
            cursor.connection.commit()
            logging.info("No changes detected. Logged to audit table.")
    except Exception as e:
        cursor.connection.rollback()
        logging.error(f"Error logging changes to audit table: {e}")

# Function to export a specific table to an Excel file
def export_table_to_excel(cursor, table_name, export_folder):
    try:
        rows, columns = fetch_table_data(cursor, table_name)
        wb = Workbook()
        ws = wb.active
        ws.title = f"{table_name} Data"
        ws.append(columns)
        for row in rows:
            ws.append(list(row))
        date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        export_path = os.path.join(export_folder, f"{table_name}_Export_{date_str}.xlsx")
        wb.save(export_path)
        logging.info(f"{table_name} table exported to: {export_path}")
    except Exception as e:
        logging.error(f"Error exporting {table_name} table to Excel: {e}")

def main():
    # Database connection parameters
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    uid = os.getenv('UID')
    pwd = os.getenv('PWD')
    export_folder = os.getenv('EXPORT_FOLDER')

    # Validate environment variables
    if not all([server, database, uid, pwd]):
        logging.error("Missing required environment variables.")
        return

    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={uid};'
        f'PWD={pwd}'
    )

    cnx = None
    try:
        cnx = pyodbc.connect(conn_str)
        cursor = cnx.cursor()

        old_rows, columns = fetch_table_data(cursor, "TblSanctionsMap")

        # Call main functions of each updater
        updaters = [cpi_main, eufatf_main, eusanctions_main, eutax_main, fatfcfa_main, fatfim_main, frsanctions_main, frtax_main, ofac_main, uksanctions_main]
        for updater_main in updaters:
            try:
                updater_main()
            except Exception as e:
                logging.error(f"Error during updater execution: {e}")

        new_rows, _ = fetch_table_data(cursor, "TblSanctionsMap")

        log_changes_to_audit_table(cursor, old_rows, new_rows, columns)
        export_table_to_excel(cursor, "TblSanctionsMap_Audit", export_folder)

        logging.info("Process completed successfully.")
    except Exception as e:
        logging.error(f"Error during processing: {e}")
    finally:
        if cnx:
            cnx.close()

if __name__ == "__main__":
    main()

