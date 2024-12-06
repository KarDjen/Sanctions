import os
import re
import requests
import pyodbc
import logging
from io import BytesIO
from unidecode import unidecode
import PyPDF2


# Load environment variables from .env file
dotenv.load_dotenv()

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retrieve database connection parameters from environment variables
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
uid = os.getenv('UID')
pwd = os.getenv('PWD')


# Define the EUSanctionsUpdater class
class EUSanctionsUpdater:

    # Initialize the EUSanctionsUpdater class with the database name
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={uid};'
            f'PWD={pwd}'
        )
        self.measures_dict = {
            re.compile(r'Asset freeze and prohibition to make funds available', re.IGNORECASE): 'EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE',
            re.compile(r'Investments', re.IGNORECASE): 'EU_INVESTMENTS',
            re.compile(r'Financial measures', re.IGNORECASE): 'EU_FINANCIAL_MEASURES'
        }
        self.expected_countries = self.get_expected_countries()

    # Normalize the country name by removing extra spaces, handling smart quotes, and applying unidecode
    def normalize_country_name(self, country_name):
        # Normalize country name by removing extra spaces, handling smart quotes, and applying unidecode
        # Add specific mappings for country names
        special_mappings = {
            "BOSNIA & HERZEGOVINA": "BOSNIA AND HERZEGOVINA"
        }
        return special_mappings.get(country_name, unidecode(country_name.strip().upper()))

    # Retrieve the expected country names from the database
    def get_expected_countries(self):
        expected_countries = set()
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute("SELECT [COUNTRY_NAME_ENG] FROM TblSanctionsMap")
            for row in cursor.fetchall():
                expected_countries.add(self.normalize_country_name(row[0]))
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error retrieving expected countries: {e}")
        return expected_countries

    # Parse the PDF from the given URL and extract the text
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

    # Extract the country names and sanctions information from the text
    def extract_country_and_sanctions(self, text):
        updates = {}
        lines = text.split("\n")
        current_country = None

        for line in lines:
            line = self.normalize_country_name(line)
            if line in self.expected_countries:
                current_country = line
                if current_country not in updates:
                    updates[current_country] = {db_column: 'NO' for db_column in self.measures_dict.values()}
            elif current_country:
                for pattern, db_column in self.measures_dict.items():
                    if re.search(pattern, line):
                        updates[current_country][db_column] = 'YES'

        return updates

    # Update the database with sanctions data
    def update_database_EUsanctions(self, updates, urls_parsed=None):
        changes_yes_to_no = []
        changes_no_to_yes = []

        if urls_parsed is None:
            urls_parsed = {}

        try:
            # Connect to the database
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Prepare dictionaries for tracking updates
            yes_update_data = {}  # Stores countries that need to be set to YES for each column
            columns_to_update = set()  # Set of columns to update

            # Step 1: Collect the "YES" updates and track which countries are being updated
            logging.info("Collecting 'YES' updates for sanctions...")
            for country_name, sanctions in updates.items():
                normalized_country_name = self.normalize_country_name(country_name)
                cursor.execute("SELECT 1 FROM TblSanctionsMap WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = ?",
                               normalized_country_name)
                result = cursor.fetchone()

                if result:
                    for db_column, status in sanctions.items():
                        if status == "YES":
                            if db_column not in yes_update_data:
                                yes_update_data[db_column] = []
                            yes_update_data[db_column].append(normalized_country_name)
                            columns_to_update.add(db_column)
                else:
                    logging.warning(f"Country {normalized_country_name} not found in the database.")

            # Step 2: Update all relevant countries to 'YES' for each sanctions column
            logging.info("Updating countries to 'YES' for sanctions...")
            for db_column, country_names in yes_update_data.items():
                if country_names:
                    placeholders = ', '.join(['?'] * len(country_names))
                    bulk_yes_update_query = f"""
                        UPDATE TblSanctionsMap
                        SET [{db_column}] = 'YES'
                        WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') IN ({placeholders})
                    """
                    cursor.execute(bulk_yes_update_query, tuple(country_names))
                    logging.info(f"Updated {len(country_names)} countries to 'YES' for {db_column}.")
                    cnx.commit()

            # Step 3: Set remaining countries to 'NO' and track changes
            logging.info("Setting remaining countries to 'NO' and tracking changes...")
            for db_column in columns_to_update:
                cursor.execute(f"""
                    SELECT [COUNTRY_NAME_ENG], [{db_column}]
                    FROM TblSanctionsMap
                    WHERE [{db_column}] = 'YES'
                """)
                countries_in_yes = cursor.fetchall()

                for row in countries_in_yes:
                    country_name, current_status = row
                    if country_name not in yes_update_data.get(db_column, []):
                        # Track change from YES to NO
                        changes_yes_to_no.append((country_name, db_column, 'YES', 'NO'))

                        # Update to NO
                        cursor.execute(f"""
                            UPDATE TblSanctionsMap
                            SET [{db_column}] = 'NO'
                            WHERE [COUNTRY_NAME_ENG] = ?
                        """, country_name)
                        logging.info(f"Set {country_name} to 'NO' for {db_column}.")

                cnx.commit()

            # Step 4: Log changes from NO to YES
            logging.info("Tracking changes from NO to YES...")
            for country_name, sanctions in updates.items():
                normalized_country_name = self.normalize_country_name(country_name)
                for db_column, new_status in sanctions.items():
                    cursor.execute(f"""
                        SELECT [{db_column}] FROM TblSanctionsMap
                        WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = ?
                    """, normalized_country_name)
                    old_status = cursor.fetchone()[0]

                    # Track changes from NO to YES
                    if old_status == 'NO' and new_status == 'YES':
                        changes_no_to_yes.append((normalized_country_name, db_column, 'NO', 'YES'))

            # Commit all changes at once
            logging.info("Database updated successfully.")

            # Step 5: Log the sanctions found for each country per URL
            if urls_parsed:
                logging.info("Sanctions parsed from each URL:")
                for url, countries_data in urls_parsed.items():
                    logging.info(f"URL: {url}")
                    for country, sanctions in countries_data.items():
                        sanctions_found = [f"{db_column}: {status}" for db_column, status in sanctions.items()]
                        logging.info(f"Country: {country}, Sanctions: {', '.join(sanctions_found)}")

        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")
        finally:
            # Ensure cursor and connection are properly closed
            cursor.close()
            cnx.close()

        # Log the countries that switched from YES to NO
        if changes_yes_to_no:
            logging.info("Countries switched from YES to NO:")
            for change in changes_yes_to_no:
                logging.info(f"Country: {change[0]}, Column: {change[1]}, Change: {change[2]} -> {change[3]}")

        # Log the countries that switched from NO to YES
        if changes_no_to_yes:
            logging.info("Countries switched from NO to YES:")
            for change in changes_no_to_yes:
                logging.info(f"Country: {change[0]}, Column: {change[1]}, Change: {change[2]} -> {change[3]}")

        return changes_yes_to_no, changes_no_to_yes

    # Check for changes in the database for EU sanctions
    def check_database_changes_EUsanctions(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Fetch the current status from the database for each measure
            for country_name, sanctions in updates.items():
                normalized_country_name = self.normalize_country_name(country_name)
                cursor.execute("SELECT 1 FROM TblSanctionsMap WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = ?", normalized_country_name)
                result = cursor.fetchone()
                if result:
                    for db_column, new_status in sanctions.items():
                        cursor.execute(f"SELECT [{db_column}] FROM TblSanctionsMap WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = ?", normalized_country_name)
                        old_status = cursor.fetchone()[0]
                        if old_status != new_status:
                            changes.append((normalized_country_name, db_column, old_status, new_status))
                            if new_status.upper() == 'YES':
                                logging.info(f"Country: {normalized_country_name}, Column: {db_column}, Old Status: {old_status}, New Status: {new_status}")

            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

def main():

    regime_ids = range(1, 71)

    updater = EUSanctionsUpdater(database)

    try:
        all_updates = {}
        for regime_id in regime_ids:
            url = f"https://www.sanctionsmap.eu/api/v1/pdf/regime?id[]={regime_id}&lang=en"
            pdf_text = updater.parse_pdf(url)
            if pdf_text:
                updates = updater.extract_country_and_sanctions(pdf_text)
                for country, sanctions in updates.items():
                    normalized_country_name = updater.normalize_country_name(country)
                    if normalized_country_name in all_updates:
                        for db_column, status in sanctions.items():
                            if status == 'YES':
                                all_updates[normalized_country_name][db_column] = 'YES'
                    else:
                        all_updates[normalized_country_name] = sanctions

        logging.info("Checking for database changes...")
        changes = updater.check_database_changes_EUsanctions(all_updates)
        if changes:
            logging.info(f"Changes detected: {changes}")

        logging.info("Updating the database with new EU sanctions data...")
        updater.update_database_EUsanctions(all_updates)

    except Exception as e:
        logging.error(f"Error during update: {e}")

if __name__ == "__main__":
    main()
