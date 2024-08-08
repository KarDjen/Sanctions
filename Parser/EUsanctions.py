import re
import requests
import pyodbc
import logging
from io import BytesIO
from unidecode import unidecode
import PyPDF2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EUSanctionsUpdater:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )
        self.measures_dict = {
            re.compile(r'Asset freeze and prohibition to make funds available', re.IGNORECASE): 'EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE',
            re.compile(r'Investments', re.IGNORECASE): 'EU_-_INVESTMENTS',
            re.compile(r'Financial measures', re.IGNORECASE): 'EU_-_FINANCIAL_MEASURES'
        }
        self.expected_countries = self.get_expected_countries()

    def get_expected_countries(self):
        # Retrieve expected country names from the database
        expected_countries = set()
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute("SELECT [COUNTRY_NAME_(ENG)] FROM TblCountries_New")
            for row in cursor.fetchall():
                expected_countries.add(unidecode(row[0].strip().upper()))
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error retrieving expected countries: {e}")
        return expected_countries

    def parse_pdf(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            pdf_file = BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            text = ""
            for page_num in range(len(pdf_reader.pages)):
                text += pdf_reader.pages[page_num].extract_text()
            return text
        return None

    def extract_country_and_sanctions(self, text):
        updates = {}
        lines = text.split("\n")
        current_country = None

        for line in lines:
            line = unidecode(line.strip().upper())
            if line in self.expected_countries:
                current_country = line
                if current_country not in updates:
                    updates[current_country] = {db_column: 'NO' for db_column in self.measures_dict.values()}
            elif current_country:
                for pattern, db_column in self.measures_dict.items():
                    if re.search(pattern, line):
                        updates[current_country][db_column] = 'YES'

        return updates

    def initialize_sanctions_to_no(self):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            update_query = f"""
                UPDATE TblCountries_New
                SET {', '.join([f'[{db_column}] = ?' for db_column in self.measures_dict.values()])}
            """
            cursor.execute(update_query, *['NO'] * len(self.measures_dict))
            cnx.commit()
            logging.info("All sanctions initialized to 'NO'.")
        except Exception as e:
            logging.error(f"Error initializing sanctions to 'NO': {e}")
        finally:
            cursor.close()
            cnx.close()

    def update_database_EUsanctions(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Apply the actual updates from parsed PDFs
            for country_name, sanctions in updates.items():
                cursor.execute(f"SELECT 1 FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?", country_name)
                result = cursor.fetchone()
                if result:
                    logging.info(f"Country {country_name} found in the database.")
                    for db_column, status in sanctions.items():
                        cursor.execute(f"SELECT [{db_column}] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?", country_name)
                        old_status = cursor.fetchone()[0]
                        if old_status != status:
                            update_query = f"""
                                UPDATE TblCountries_New
                                SET [{db_column}] = ?
                                WHERE [COUNTRY_NAME_(ENG)] = ?
                            """
                            logging.info(f"Updating {db_column} for {country_name} to {status}")
                            cursor.execute(update_query, status, country_name)
                            changes.append((country_name, db_column, old_status, status))
                else:
                    logging.warning(f"Country {country_name} NOT found in the database.")
                # Log all sanctions found for the country
                sanctions_info = ', '.join([f"{db_column}: {status}" for db_column, status in sanctions.items()])
                logging.info(f"Sanctions found for {country_name}: {sanctions_info}")
            cnx.commit()
            logging.info("Database updated successfully.")
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")
        finally:
            cursor.close()
            cnx.close()
        return changes

    def check_database_changes_EUsanctions(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Fetch the current status from the database for each measure
            for country_name, sanctions in updates.items():
                country_name_upper = unidecode(country_name.strip().upper())
                cursor.execute("SELECT 1 FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?", country_name_upper)
                result = cursor.fetchone()
                if result:
                    for db_column, new_status in sanctions.items():
                        cursor.execute(f"SELECT [{db_column}] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?", country_name_upper)
                        old_status = cursor.fetchone()[0]
                        if old_status != new_status:
                            changes.append((country_name, db_column, old_status, new_status))
                            if new_status.upper() == 'YES':
                                logging.info(f"Country: {country_name}, Column: {db_column}, Old Status: {old_status}, New Status: {new_status}")

            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

def main():
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'
    regime_ids = range(1, 71)  # Regime IDs from 1 to 70

    updater = EUSanctionsUpdater(db_name)

    try:
        # Initialize all sanctions to 'NO'
        updater.initialize_sanctions_to_no()

        # Collect updates for EU sanctions from each regime
        all_updates = {}
        for regime_id in regime_ids:
            url = f"https://www.sanctionsmap.eu/api/v1/pdf/regime?id[]={regime_id}&lang=en"
            pdf_text = updater.parse_pdf(url)
            if pdf_text:
                updates = updater.extract_country_and_sanctions(pdf_text)
                for country, sanctions in updates.items():
                    if country in all_updates:
                        for db_column, status in sanctions.items():
                            if status == 'YES':
                                all_updates[country][db_column] = 'YES'
                    else:
                        all_updates[country] = sanctions
                    sanctions_info = ', '.join([f"{db_column}: {status}" for db_column, status in sanctions.items()])
                    logging.info(f"Parsed country from regime ID {regime_id}: {country} with sanctions: {sanctions_info}")

        # Check database changes for EU sanctions
        logging.info("Checking for database changes...")
        changes = updater.check_database_changes_EUsanctions(all_updates)
        if changes:
            logging.info(f"Changes detected: {changes}")

        # Update database for EU sanctions
        logging.info("Updating the database with new EU sanctions data...")
        updater.update_database_EUsanctions(all_updates)

    except Exception as e:
        logging.error(f"Error during update: {e}")

if __name__ == "__main__":
    main()
