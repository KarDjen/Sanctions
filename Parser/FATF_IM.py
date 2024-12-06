"""
This script is used to update the FATF IM data in the database. It scrapes the FATF website for the latest sanctions data.
It then updates the database with the new data and checks for any changes. The changes are logged for auditing purposes.
"""
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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retrieve database connection parameters from environment variables
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
uid = os.getenv('UID')
pwd = os.getenv('PWD')

# This class is used to update the FATF IM data in the database
class FATFIMUpdater:

    # Initialize the updater with the database name and connection string
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

    # Build the URL for the latest FATF IM page based on the current date
    def build_url(self):
        base_url = 'https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/increased-monitoring-{}-{}.html'
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

    # Normalize the country name for consistency
    def normalize_country_name(self, name):
        # Normalize country name by converting to uppercase and replacing smart quotes and apostrophes
        return unidecode(name.strip().upper().replace('’', "'"))

    # Parse the HTML content to extract the high-risk countries
    def parse_html(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            countries = []

            start_tag = soup.find('h6', class_='cmp-title__text', string='Country')
            if not start_tag:
                return None

            countries_div = start_tag.find_next('p')
            if not countries_div:
                return None

            countries_text = countries_div.get_text()
            countries_list = [self.normalize_country_name(country) for country in countries_text.split(', ')]

            # Country mapping for consistency with the database
            country_mapping = {
                'MYANMAR (BURMA)': 'MYANMAR',
                'DEMOCRATIC PEOPLE\'S REPUBLIC OF KOREA (DPRK - NORTH KOREA)': 'NORTH KOREA',
                'CROATIA, DEMOCRATIC REPUBLIC OF THE CONGO': ['CROATIA', 'DEMOCRATIC REPUBLIC OF THE CONGO'],
                "COTE D'IVOIRE": "IVORY COAST"
            }

            mapped_countries = []
            for country_name in countries_list:
                if country_name in country_mapping:
                    if isinstance(country_mapping[country_name], list):
                        mapped_countries.extend(country_mapping[country_name])
                    else:
                        mapped_countries.append(country_mapping[country_name])
                else:
                    mapped_countries.append(country_name)

            return mapped_countries
        return None


    # Update the database with the FATF IM data
    def update_database_FATF_IM(self, high_risk_countries):
        try:
            # Establish connection to the database
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:

                    # Step 1: Drop dependent computed columns
                    logging.info("Dropping dependent computed columns...")
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
                    logging.info("Dropped computed columns successfully.")

                    # Step 2: Ensure the column structure is correct for FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING
                    logging.info("Ensuring column structure for FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING is correct...")
                    cursor.execute("""
                        ALTER TABLE TblSanctionsMap
                        ALTER COLUMN [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] NVARCHAR(50)
                    """)
                    cnx.commit()
                    logging.info("Column structure updated successfully.")

                    # Step 3: Bulk update specified high-risk countries to 'YES'
                    if high_risk_countries:
                        normalized_country_names = [self.normalize_country_name(country) for country in high_risk_countries]

                        # Process the updates in batches
                        batch_size = 500
                        logging.info(f"Processing {len(normalized_country_names)} countries in batches of {batch_size}...")

                        for i in range(0, len(normalized_country_names), batch_size):
                            batch = normalized_country_names[i:i + batch_size]
                            placeholders = ', '.join(['?'] * len(batch))

                            # Update the countries in the list to 'YES'
                            update_yes_query = f"""
                                UPDATE TblSanctionsMap
                                SET [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES'
                                WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') IN ({placeholders})
                            """
                            cursor.execute(update_yes_query, tuple(batch))
                            logging.info(f"Updated {len(batch)} countries to 'YES' for FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING.")
                            cnx.commit()

                        # Step 4: Set remaining countries that are NOT in the parsed list to 'NO'
                        logging.info("Updating countries that are NOT in the high-risk list to 'NO'...")
                        if normalized_country_names:
                            placeholders = ', '.join(['?'] * len(normalized_country_names))
                            update_no_query = f"""
                                UPDATE TblSanctionsMap
                                SET [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO'
                                WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') NOT IN ({placeholders})
                            """
                            cursor.execute(update_no_query, tuple(normalized_country_names))
                            logging.info("Set remaining countries to 'NO' for FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING.")
                            cnx.commit()

                    # Step 5: Recreate the computed columns
                    logging.info("Recreating computed columns...")
                    cursor.execute(get_sanctions_map_columns_sql())
                    cnx.commit()
                    logging.info("Recreated computed columns successfully.")

        except pyodbc.Error as e:
            logging.error(f"Database error during FATF IM updates: {e}")
        except Exception as e:
            logging.error(f"General error during FATF IM updates: {e}")

    # Check for changes in the database
    def check_database_changes_FATF_IM(self, updates):
        changes = []
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    for country_name, new_status in updates:
                        normalized_country_name = self.normalize_country_name(country_name)
                        cursor.execute(
                            "SELECT [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] FROM TblSanctionsMap WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = ?",
                            normalized_country_name)
                        result = cursor.fetchone()
                        if result:
                            old_status = result[0] if result[0] is not None else 'NO'
                            if old_status != new_status:
                                changes.append((country_name, 'FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING', old_status, new_status))
                                logging.info(f"Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes


def main():
    # Initialize the FATF IM updater

    # Create an instance of the FATF IM updater
    updater = FATFIMUpdater(database)

    # Build URL for the latest available increased monitoring page
    url = updater.build_url()

    # Check if a valid URL was found
    if url:
        high_risk_countries = updater.parse_html(url)
        if high_risk_countries:
            logging.info(f"High-risk countries found: {', '.join(high_risk_countries)}")

            # Prepare updates based on the high-risk countries
            updates = [(country, 'YES') for country in high_risk_countries]

            # Update the database based on the updates
            logging.info("Updating the database with new FATF IM data...")
            updater.update_database_FATF_IM(high_risk_countries)

            # Check for changes in the database
            changes = updater.check_database_changes_FATF_IM(updates)
            if changes:
                logging.info("Changes detected in the database:")
                for change in changes:
                    logging.info(f"Country: {change[0]}, Old Status: {change[2]}, New Status: {change[3]}")
        else:
            logging.info("No high-risk countries found or failed to parse the HTML content.")
    else:
        logging.error("No valid URL found for increased monitoring data.")


if __name__ == "__main__":
    main()
