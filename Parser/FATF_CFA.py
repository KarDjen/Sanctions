"""
This script is used to update the FATF CFA data in the TblSanctionsMap table.
It fetches the latest FATF CFA data from the FATF website and updates the database accordingly.
"""

# Importing required libraries
import os
import dotenv
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging
from datetime import datetime
from Logic.ComputedLogic import get_sanctions_map_columns_sql


# Load environment variables from .env file
dotenv.load_dotenv()

# Setting up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retrieve database connection parameters from environment variables
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
uid = os.getenv('UID')
pwd = os.getenv('PWD')

# Defining the FATFCFAUpdater class
class FATFCFAUpdater:

    # Constructor to initialize the database name and connection string
    def __init__(self, db_name):
        # Ensure db_name is passed as a string and not as an object reference
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={uid};'
            f'PWD={pwd}'
        )

    # Method to normalize the country name
    def normalize_country_name(self, name):
        # Normalize the country name by stripping extra spaces, replacing smart quotes, and applying unidecode for uniformity
        name = unidecode(name.strip().upper())
        name = name.replace('’', "'")  # Replace smart quotes with regular quotes
        return name

    # Method to build the URL for the latest FATF CFA data

    def build_url(self):
            base_url = 'https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/Call-for-action-{}-{}.html'
            current_date = datetime.now()
            year = current_date.year
            month = current_date.month

            # Determine the most recent FATF report month
            if month >= 10:
                report_month = 'october'
            elif month >= 6:
                report_month = 'june'
            elif month >= 2:
                report_month = 'february'
            else:
                # If before February, use the previous year's October report
                report_month = 'october'
                year -= 1

            # Construct the URL
            url = base_url.format(report_month, year)

            # Check if the URL exists
            response = requests.head(url)
            if response.status_code == 200:
                logging.info(f"Found valid URL: {url}")
                return url
            else:
                logging.error(f"URL not found: {url}")
                return None

    # Method to parse the HTML content and extract the high-risk countries
    def parse_html(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            countries = []

            titles = soup.find_all('h3') # Extracting all h3 tags
            for title in titles:
                b_tag = title.find('b') # Extracting the b tag from the h3 tag
                if b_tag:
                    country_name = self.normalize_country_name(b_tag.text)
                    # Mapping countries with specific names
                    if country_name == 'MYANMAR':
                        country_name = 'MYANMAR (BURMA)'
                    elif country_name == "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA (DPRK)":
                        country_name = "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA (DPRK - NORTH KOREA)"
                    elif country_name == 'CROATIA, DEMOCRATIC REPUBLIC OF THE CONGO':
                        countries.append(self.normalize_country_name('CROATIA'))
                        countries.append(self.normalize_country_name('REPUBLIC DEMOCRATIC OF THE CONGO'))
                        continue
                    countries.append(country_name)

            return countries
        return None

    # Method to drop computed columns if they exist
    def drop_computed_columns(self, cursor):
        try:
            # Check if columns exist before attempting to drop them
            columns_to_drop = ['LEVEL_OF_RISK', 'LEVEL_OF_VIGILANCE', 'LIST']

            for column in columns_to_drop:
                # Query to check if the column exists in the table
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'TblSanctionsMap' AND COLUMN_NAME = '{column}'
                """)
                column_exists = cursor.fetchone()[0]

                if column_exists:
                    logging.info(f"Dropping computed column {column}...")
                    cursor.execute(f"ALTER TABLE TblSanctionsMap DROP COLUMN [{column}]")
                    logging.info(f"Successfully dropped column {column}")
                else:
                    logging.info(f"Column {column} does not exist, skipping drop.")

        except pyodbc.Error as e:
            logging.error(f"Error dropping computed columns: {e}")

    # Method to update the database with the latest FATF CFA data
    def update_database_FATF_CFA(self, high_risk_countries):
        try:
            # Establishing the connection
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Drop dependent computed columns if they exist
            logging.info("Dropping dependent computed columns if they exist...")
            self.drop_computed_columns(cursor)  # Assuming self.drop_computed_columns() is a method in your class
            cnx.commit()

            # Alter the FATF column to ensure it can store the correct values
            logging.info("Altering the FATF column...")
            cursor.execute("""
                ALTER TABLE TblSanctionsMap
                ALTER COLUMN [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] NVARCHAR(50)
            """)
            cnx.commit()

            # Normalize the country names in the high-risk list
            normalized_countries = [self.normalize_country_name(country) for country in high_risk_countries]

            # Step 1: Bulk update high-risk countries to 'YES'
            if normalized_countries:
                placeholders = ', '.join('?' for _ in normalized_countries)
                logging.info("Bulk updating high-risk countries to 'YES'...")
                bulk_update_yes_query = f"""
                    UPDATE TblSanctionsMap
                    SET [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES'
                    WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') IN ({placeholders})
                """
                cursor.execute(bulk_update_yes_query, normalized_countries)
                logging.info(f"Updated the following countries to 'YES': {normalized_countries}")
                cnx.commit()

            # Step 2: Set all other countries to 'NO'
            logging.info("Setting remaining countries to 'NO'...")
            if normalized_countries:
                placeholders = ', '.join('?' for _ in normalized_countries)
                update_no_query = f"""
                    UPDATE TblSanctionsMap
                    SET [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO'
                    WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') NOT IN ({placeholders})
                """
                cursor.execute(update_no_query, normalized_countries)
                logging.info("Set remaining countries to 'NO'.")
            else:
                # If no high-risk countries were provided, set all countries to 'NO'
                update_no_query = """
                    UPDATE TblSanctionsMap
                    SET [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO'
                """
                cursor.execute(update_no_query)
                logging.info("Set all countries to 'NO' as no high-risk countries were provided.")

            cnx.commit()

            # Step 3: Recreate computed columns
            logging.info("Recreating computed columns LEVEL_OF_RISK, LEVEL_OF_VIGILANCE, LIST...")
            cursor.execute(get_sanctions_map_columns_sql())
            cnx.commit()

            # Close the cursor and connection
            cursor.close()
            cnx.close()

            logging.info("FATF CFA database updated successfully.")

        except pyodbc.Error as e:
            logging.error(f"Database error during FATF CFA updates: {e}")
        except Exception as e:
            logging.error(f"General error during FATF CFA updates: {e}")

    # Method to check for changes in the database
    def check_database_changes_FATFCFA(self, high_risk_countries):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Fetch the current status from the database
            cursor.execute("SELECT [COUNTRY_NAME_ENG], [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] FROM TblSanctionsMap")
            all_countries = cursor.fetchall()

            # Create a set of high-risk countries for quick lookup
            high_risk_set = {self.normalize_country_name(country) for country in high_risk_countries}

            for country_row in all_countries:
                country_name = self.normalize_country_name(country_row[0])
                old_status = country_row[1] if country_row[1] is not None else 'NO'
                new_status = 'YES' if country_name in high_risk_set else 'NO'

                if old_status != new_status:
                    changes.append((country_name, old_status, new_status))
                    if new_status == 'YES':
                        logging.info(f"Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")

            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

def main():


    # Create an instance of the FATFCFAUpdater class
    updater = FATFCFAUpdater(database)

    # Build URL for the latest available call for action page
    url = updater.build_url()
    if url:
        # Parse the HTML to get the high-risk countries
        high_risk_countries = updater.parse_html(url)
        if high_risk_countries:
            logging.info(f"High-risk countries found: {', '.join(high_risk_countries)}")

            # Check database changes
            logging.info("Checking for database changes...")
            changes = updater.check_database_changes_FATFCFA(high_risk_countries)
            if changes:
                logging.info(f"Changes detected: {changes}")

            # Update the database based on the high-risk countries
            logging.info("Updating the database with new FATF CFA data...")
            updater.update_database_FATF_CFA(high_risk_countries)
        else:
            logging.error("No high-risk countries found or failed to parse the HTML content.")
    else:
        logging.error("No valid URL found for call for action data.")


if __name__ == "__main__":
    main()

