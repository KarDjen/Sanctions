"""
This script is the main entry point for the Sanctions Matrix Updater.
It calls the main functions of each parser and logs changes to the audit table.
It also exports the updated database to an Excel file for review.
It is intended to be run as a scheduled task or cron job.
"""

# Import required modules
import os
import datetime
import logging
import pyodbc
from openpyxl import Workbook
# from Email.email_sender import send_email
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
        # Flag to check if any changes were detected
        changes_detected = False

        for old_row, new_row in zip(old_rows, new_rows):
            country_name = old_row[columns.index('COUNTRY_NAME_ENG')]

            for i, column in enumerate(columns):
                old_value = old_row[i]
                new_value = new_row[i]

                # Only log the change if the old value is different from the new value
                if old_value != new_value:
                    # Set the flag to true if a change is detected
                    changes_detected = True

                    # Log the specific change for this column
                    audit_sql = """
                        INSERT INTO TblSanctionsMap_Audit (
                            CountryName, ColumnName, OldValue, NewValue, UpdatedAt
                        ) VALUES (?, ?, ?, ?, ?)
                    """
                    # Insert the change log into the audit table
                    cursor.execute(audit_sql, country_name, column, old_value, new_value, datetime.datetime.now())
                    logging.info(f"Logged change for {country_name} in column {column}: {old_value} -> {new_value}")

        # Commit changes to the audit table
        cursor.connection.commit()

        # Insert a log if no changes were detected
        if not changes_detected:
            audit_sql = """
                INSERT INTO TblSanctionsMap_Audit (
                    CountryName, ColumnName, OldValue, NewValue, UpdatedAt
                ) VALUES (?, ?, ?, ?, ?)
            """
            # Record a general message indicating no changes
            cursor.execute(audit_sql, 'ALL', 'None', 'No changes detected', 'No changes detected', datetime.datetime.now())
            cursor.connection.commit()
            logging.info("No changes detected. Logged to audit table.")

    except Exception as e:
        logging.error(f"Error logging changes to audit table: {e}")

# Function to export the database to an Excel file
def export_database_to_excel(db_name, export_folder):
    try:
        conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )
        cnx = pyodbc.connect(conn_str)
        cursor = cnx.cursor()

        # Fetch data from the TblSanctionsMap table
        rows, columns = fetch_table_data(cursor, "TblSanctionsMap")

        # Create a new Excel workbook and add data
        wb = Workbook()
        ws = wb.active
        ws.title = "Countries Data"

        # Write column headers
        ws.append(columns)

        # Write data rows
        for row in rows:
            ws.append(list(row))

        # Save the workbook with a timestamp in the filename
        date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        export_path = os.path.join(export_folder, f"Sanctions_Matrix_{date_str}.xlsx")
        wb.save(export_path)

        logging.info(f"Database exported to: {export_path}")

        cursor.close()
        cnx.close()
    except Exception as e:
        logging.error(f"Error exporting database to Excel: {e}")

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
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'

    # Folder to export Excel files
    export_folder = 'A:\\Compliance\\AML-KYC\\Sanctions_excel_output'

    cnx = None
    try:
        conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )
        cnx = pyodbc.connect(conn_str)
        cursor = cnx.cursor()

        # Fetch old data before updates
        old_rows, columns = fetch_table_data(cursor, "TblSanctionsMap")

        # Call main functions of each updater
        updaters = [cpi_main, eufatf_main, eusanctions_main, eutax_main, fatfcfa_main, fatfim_main, frsanctions_main, frtax_main, ofac_main, uksanctions_main]
        for updater_main in updaters:
            try:
                updater_main()
            except Exception as e:
                logging.error(f"Error during updater execution: {e}")

        # Fetch new data after updates
        new_rows, _ = fetch_table_data(cursor, "TblSanctionsMap")

        # Log changes to the audit table
        log_changes_to_audit_table(cursor, old_rows, new_rows, columns)
        logging.info("Changes logged to audit table.")
        # Commit changes to the database
        cnx.commit()

        # Export updated data
        export_database_to_excel(db_name, export_folder)
        export_table_to_excel(cursor, "TblSanctionsMap_Audit", export_folder)


        logging.info("Process completed successfully.")

    except Exception as e:
        logging.error(f"Error during processing: {e}")
        if cnx:
            cnx.rollback()

    finally:
        if cnx:
            cnx.close()

if __name__ == "__main__":
    main()
