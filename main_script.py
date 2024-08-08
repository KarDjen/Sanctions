import os
import datetime
import logging
import pyodbc
from io import StringIO
from openpyxl import Workbook
from Email.email_sender import send_email
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_email_template(template_path):
    with open(template_path, 'r') as file:
        return file.read()

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

        # Query to get all data from the TblCountries_New table
        cursor.execute("SELECT * FROM TblCountries_New")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

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

def main():
    # File paths and database name
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'
    email_template_path = 'Email\\email_template.txt'
    export_folder = 'A:\\Compliance\\AML-KYC\\Sanctions_excel_output'

    # Set up in-memory log capture
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)

    # Call main functions of each updater
    all_changes = []
    updaters = [
        cpi_main, eufatf_main, eusanctions_main, eutax_main,
        fatfcfa_main, fatfim_main, frsanctions_main, frtax_main,
        ofac_main, uksanctions_main
    ]

    for updater_main in updaters:
        try:
            changes = updater_main()
            if changes:
                all_changes.extend(changes)
        except Exception as e:
            logging.error(f"Error during updater execution: {e}")

    # Capture logs
    log_contents = log_stream.getvalue()
    logging.getLogger().removeHandler(handler)

    # Prepare email content
    template = read_email_template(email_template_path)
    changes_summary = "\n".join(
        [f"Country: {change[0]}, Column: {change[1]}, Old Value: {change[2]}, New Value: {change[3]}" for change in all_changes])

    subject = "Log details - Sanctions updates"

    body = template.replace("{{log_details}}", log_contents)

    # Send email
    to_emails = ['karim.djenadi@axiom-ai.com']
    send_email(subject, body, to_emails)

    # Export the updated database to an Excel file
    export_database_to_excel(db_name, export_folder)

    logging.info("Process completed successfully.")

if __name__ == "__main__":
    main()
