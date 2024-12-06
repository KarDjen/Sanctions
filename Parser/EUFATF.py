"""
This script is used to parse the EU FATF website and update the database with the high-risk countries.
It also checks for changes in the database and logs the changes.
"""

# Importing required libraries
import os
import dotenv
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging
from Logic.ComputedLogic import get_sanctions_map_columns_sql


# Load environment variables from .env file
dotenv.load_dotenv()

# Setting up the logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

server = os.getenv('SERVER')
database = os.getenv('DATABASE')
uid = os.getenv('UID')
pwd = os.getenv('PWD')

# Defining the EUFATFUpdater class
class EUFATFUpdater:

    # Constructor to initialize the database name, connection string, updates, and changes
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={uid};'
            f'PWD={pwd}'
        )
        self.updates = []
        self.changes = []

    # Method to normalize the country name
    def normalize_country_name(self, country_name):
        # Normalize country name by removing extra spaces, handling smart quotes, and applying unidecode
        country_name = unidecode(country_name.strip().upper())
        country_name = country_name.replace('’', "'")  # Replace smart quotes with standard quotes
        return country_name

    # Method to parse the HTML content of the EU FATF website
    def parse_html(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            countries = []

            # Find the table with the high-risk countries
            table = soup.find('table', {'class': 'ecl-table'})
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if cols:
                        country_name = self.normalize_country_name(cols[0].text.strip().upper())
                        # Mapping countries with specific names
                        if country_name == 'MYANMAR':
                            country_name = 'MYANMAR (BURMA)'
                        elif country_name == 'NORTH KOREA':
                            country_name = "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA (DPRK - NORTH KOREA)"
                        countries.append(country_name)

            return countries
        return None

    # Method to update the database with the EU FATF data
    def update_database_EUFATF(self, updates):
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:

                    # Track the countries before updating
                    changes_yes_to_no = []
                    changes_no_to_yes = []

                    # Drop dependent computed columns (if necessary)
                    logging.info("Dropping computed columns if they exist...")
                    cursor.execute("""
                        IF EXISTS (SELECT 1 
                                   FROM sys.columns 
                                   WHERE name IN ('LEVEL_OF_RISK', 'LEVEL_OF_VIGILANCE', 'LIST') 
                                   AND object_id = OBJECT_ID('TblSanctionsMap'))
                        BEGIN
                            ALTER TABLE TblSanctionsMap 
                            DROP COLUMN LEVEL_OF_RISK, LEVEL_OF_VIGILANCE, LIST;
                        END
                    """)
                    cnx.commit()

                    # Alter the column (if necessary)
                    logging.info("Altering column [EU_AML_HIGH_RISK_COUNTRIES]...")
                    cursor.execute("""
                        ALTER TABLE TblSanctionsMap
                        ALTER COLUMN [EU_AML_HIGH_RISK_COUNTRIES] VARCHAR(3)
                    """)
                    cnx.commit()

                    # Step 1: Track the current state of all countries before the update
                    logging.info("Fetching current country statuses...")
                    cursor.execute("SELECT [COUNTRY_NAME_ENG], [EU_AML_HIGH_RISK_COUNTRIES] FROM TblSanctionsMap")
                    country_status_before = {row[0]: row[1] for row in cursor.fetchall()}

                    # Step 2: Update the countries with high-risk status to 'YES'
                    logging.info("Updating high-risk countries to 'YES'...")
                    if updates:
                        country_names_yes = [self.normalize_country_name(country) for country, status in updates if
                                             status == 'YES']

                        if country_names_yes:
                            placeholders = ', '.join('?' for _ in country_names_yes)
                            update_yes_query = f"""
                                UPDATE TblSanctionsMap
                                SET [EU_AML_HIGH_RISK_COUNTRIES] = 'YES'
                                WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') IN ({placeholders})
                            """
                            cursor.execute(update_yes_query, country_names_yes)
                            logging.info(f"Set countries to 'YES': {country_names_yes}")
                        cnx.commit()

                    # Step 3: Set the remaining countries to 'NO'
                    logging.info("Setting remaining countries to 'NO'...")
                    if country_names_yes:
                        placeholders = ', '.join('?' for _ in country_names_yes)
                        update_no_query = f"""
                            UPDATE TblSanctionsMap
                            SET [EU_AML_HIGH_RISK_COUNTRIES] = 'NO'
                            WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') NOT IN ({placeholders})
                        """
                        cursor.execute(update_no_query, country_names_yes)
                        logging.info("Set remaining countries to 'NO'.")
                    else:
                        # If there are no 'YES' countries, set all countries to 'NO'
                        logging.info("No countries to set to 'YES', setting all to 'NO'.")
                        cursor.execute("""
                            UPDATE TblSanctionsMap
                            SET [EU_AML_HIGH_RISK_COUNTRIES] = 'NO'
                        """)
                    cnx.commit()

                    # Step 4: Compare the new state and track changes
                    logging.info("Fetching updated country statuses...")
                    cursor.execute("SELECT [COUNTRY_NAME_ENG], [EU_AML_HIGH_RISK_COUNTRIES] FROM TblSanctionsMap")
                    country_status_after = {row[0]: row[1] for row in cursor.fetchall()}

                    for country, old_status in country_status_before.items():
                        new_status = country_status_after.get(country)
                        if old_status == 'YES' and new_status == 'NO':
                            changes_yes_to_no.append(country)
                        elif old_status == 'NO' and new_status == 'YES':
                            changes_no_to_yes.append(country)

                    # Step 5: Log the changes
                    if changes_yes_to_no:
                        logging.info("Countries switched from YES to NO:")
                        for country in changes_yes_to_no:
                            logging.info(f"Country: {country} switched from YES to NO")

                    if changes_no_to_yes:
                        logging.info("Countries switched from NO to YES:")
                        for country in changes_no_to_yes:
                            logging.info(f"Country: {country} switched from NO to YES")

                    # Step 6: Recreate the computed columns (if necessary)
                    logging.info("Recreating computed columns...")
                    cursor.execute(get_sanctions_map_columns_sql())
                    cnx.commit()
                    logging.info("Computed columns recreated successfully.")

                logging.info("EU FATF database updated successfully.")

        except pyodbc.Error as e:
            logging.error(f"Database error during EU FATF updates: {e}")
        except Exception as e:
            logging.error(f"General error during EU FATF updates: {e}")

    # Method to check for changes in the database
    def check_database_changes_EUFATF(self, updates):
        changes = []
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    logging.info("Checking for database changes...")
                    for country_name, new_high_risk_status in updates:
                        normalized_country_name = self.normalize_country_name(country_name)
                        cursor.execute("""
                            SELECT [EU_AML_HIGH_RISK_COUNTRIES] 
                            FROM TblSanctionsMap 
                            WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = ?
                        """, normalized_country_name)
                        result = cursor.fetchone()

                        if result:
                            old_high_risk_status = result[0] if result[0] is not None else 'NO'
                            if old_high_risk_status != new_high_risk_status:
                                changes.append((normalized_country_name, old_high_risk_status, new_high_risk_status))
                                logging.info(
                                    f"Country: {normalized_country_name}, Old Status: {old_high_risk_status}, New Status: {new_high_risk_status}"
                                )
            logging.info("Database changes check completed.")
        except pyodbc.Error as e:
            logging.error(f"Error checking database changes: {e}")
        except Exception as e:
            logging.error(f"General error checking database changes: {e}")

        return changes

def main():


    # URL of the EU FATF website
    html_url = 'https://finance.ec.europa.eu/financial-crime/anti-money-laundering-and-countering-financing-terrorism-international-level_en'

    updater = EUFATFUpdater(database)

    # Parse the HTML to get the high-risk countries
    high_risk_countries = updater.parse_html(html_url)
    if high_risk_countries:
        logging.info(f"\nHigh-risk countries found: {', '.join(high_risk_countries)}")

        # Prepare updates based on the high-risk countries
        updates = [(country, 'YES') for country in high_risk_countries]

        # Update the database based on the updates
        logging.info("Updating the database with new EUFATF data...")
        updater.update_database_EUFATF(updates)

        # Check for changes in the database
        changes = updater.check_database_changes_EUFATF(updates)
        if changes:
            logging.info("\nChanges detected in the database:")
            for change in changes:
                logging.info(f"Country: {change[0]}, Old High-Risk Status: {change[1]}, New High-Risk Status: {change[2]}")

    else:
        logging.info("No high-risk countries found or failed to parse the HTML content.")

if __name__ == "__main__":
    main()
