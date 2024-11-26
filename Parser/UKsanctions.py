"""
This script is used to parse the UK financial sanctions list and update the SQL database with the new data.
It also checks for changes in the database and logs the changes.
"""

import re
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging
from Logic.ComputedLogic import get_sanctions_map_columns_sql

#
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# The UKSanctionsUpdater class is responsible for updating the UK financial sanctions data in the SQL database.
class UKSanctionsUpdater:

    # The __init__ method initializes the UKSanctionsUpdater object with the database name and connection string.
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )
        self.updates = []
        self.changes = []

    # The clean_country_name method cleans and normalizes country names by removing parentheses and handling apostrophes.
    def clean_country_name(self, name):
        # Clean and normalize country names by removing parentheses and handling apostrophes
        cleaned_name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        return [unidecode(n.strip().upper().replace('’', "'")) for n in cleaned_name.split(',')]

    # The map_country_name method maps specific country names to ensure consistency with the database.
    def map_country_name(self, country_name):
        # Handle specific country mappings for consistency with database
        country_name = country_name.replace('’', "'")
        mapping = {
            'MYANMAR (BURMA)': 'MYANMAR',
            'TURKIYE': 'TÜRKIYE',
            'ESWATINI': 'SWAZILAND',
            "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA": "DEMOCRATIC PEOPLE’S REPUBLIC OF KOREA (DPRK - NORTH KOREA)",
            'REPUBLIC OF GUINEA-BISSAU': 'GUINEA-BISSAU',
            'IRAN': 'IRAN RELATING TO NUCLEAR WEAPONS',
            'LEBANON (Assassination of Rafiq Hariri and others)': 'LEBANON'
        }
        return mapping.get(country_name, country_name)

    # Scrape the UK financial sanctions webpage to extract the sanctioned countries.
    def parse_financial_sanctions(self, url):

        # Scrape the UK financial sanctions webpage to extract the sanctioned countries
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            sanctioned_countries = []

            # Locate and extract country names from the sanctions list
            items = soup.find_all('div', {'class': 'gem-c-document-list__item-title'}) # Find all list items
            for item in items:
                if item.a and 'Financial sanctions' in item.a.text:
                    country_text = item.a.text.split('Financial sanctions,')[-1].strip()
                    sanctioned_countries.extend(self.clean_country_name(country_text))

            # Map the country names for consistency with the database
            sanctioned_countries = [self.map_country_name(country) for country in sanctioned_countries]
            return sanctioned_countries
        return []

    # Compare a country name with a target country, handling special cases.
    def match_country_name(self, country_name, target_country):
        # Special handling for certain country names with apostrophe discrepancies
        if country_name == "DEMOCRATIC PEOPLE’S REPUBLIC OF KOREA (DPRK - NORTH KOREA)":
            pattern = re.compile(r"DEMOCRATIC PEOPLE['’]S REPUBLIC OF KOREA", re.IGNORECASE)
            return bool(pattern.search(target_country))
        else:
            return country_name == target_country

    # Retrieve the current status of the sanctioned countries from the database.
    def collect_updates(self, sanctioned_countries):
        updates = []
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    # Retrieve all country names from the database
                    cursor.execute("SELECT [COUNTRY_NAME_ENG] FROM TblSanctionsMap")
                    all_countries = cursor.fetchall()

                    for country_row in all_countries:
                        country_name = self.map_country_name(unidecode(country_row[0].strip().upper().replace('’', "'")))
                        status = 'YES' if any(self.match_country_name(sanctioned_country, country_name) for sanctioned_country in sanctioned_countries) else 'NO'
                        updates.append((country_name, status))
        except Exception as e:
            logging.error(f"Error collecting updates: {e}")
        return updates

    # Update the database with the new UK sanctions data.
    def update_database_UKsanctions(self, updates):
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:

                    # Drop computed columns temporarily
                    logging.info("Dropping dependent computed columns...")
                    cursor.execute("""
                        IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TblSanctionsMap' AND COLUMN_NAME = 'LEVEL_OF_RISK')
                            ALTER TABLE TblSanctionsMap DROP COLUMN LEVEL_OF_RISK;
                        IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TblSanctionsMap' AND COLUMN_NAME = 'LEVEL_OF_VIGILANCE')
                            ALTER TABLE TblSanctionsMap DROP COLUMN LEVEL_OF_VIGILANCE;
                        IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TblSanctionsMap' AND COLUMN_NAME = 'LIST')
                            ALTER TABLE TblSanctionsMap DROP COLUMN LIST;
                    """)
                    cnx.commit()
                    logging.info("Dropped computed columns successfully.")

                    # Alter the column to ensure it can handle the data properly
                    logging.info("Altering column structure...")
                    cursor.execute("""
                        ALTER TABLE TblSanctionsMap
                        ALTER COLUMN [UK_FINANCIAL_SANCTIONS] NVARCHAR(50)
                    """)
                    cnx.commit()
                    logging.info("Altered column [UK_FINANCIAL_SANCTIONS] successfully.")

                    # Set specified countries to 'YES' in bulk
                    if updates:
                        # Extract countries to update to 'YES'
                        yes_countries = [country_name for country_name, status in updates if status == 'YES']

                        # Process in batches to avoid exceeding parameter limits
                        batch_size = 500
                        logging.info(f"Processing {len(yes_countries)} countries in batches of {batch_size}...")

                        for i in range(0, len(yes_countries), batch_size):
                            batch = yes_countries[i:i + batch_size]
                            placeholders = ', '.join(['?'] * len(batch))
                            update_yes_query = f"""
                                UPDATE TblSanctionsMap
                                SET [UK_FINANCIAL_SANCTIONS] = 'YES'
                                WHERE [COUNTRY_NAME_ENG] IN ({placeholders})
                            """
                            cursor.execute(update_yes_query, tuple(batch))
                            cnx.commit()
                            logging.info(f"Updated {len(batch)} countries to 'YES' in the batch.")

                    # Set countries that are not in the 'YES' list to 'NO'
                    logging.info("Setting remaining countries to 'NO'...")
                    if yes_countries:
                        placeholders = ', '.join(['?'] * len(yes_countries))
                        update_no_query = f"""
                            UPDATE TblSanctionsMap
                            SET [UK_FINANCIAL_SANCTIONS] = 'NO'
                            WHERE [COUNTRY_NAME_ENG] NOT IN ({placeholders})
                        """
                        cursor.execute(update_no_query, tuple(yes_countries))
                        cnx.commit()
                        logging.info("Set remaining countries to 'NO' successfully.")

                    # Recreate computed columns
                    logging.info("Recreating computed columns...")
                    cursor.execute(get_sanctions_map_columns_sql())
                    cnx.commit()
                    logging.info("Recreated computed columns successfully.")

        except pyodbc.Error as e:
            logging.error(f"Database error during UK sanctions updates: {e}")
        except Exception as e:
            logging.error(f"General error during UK sanctions updates: {e}")

    def check_database_changes_UKsanctions(self, updates):
        changes = []
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    # Check for changes between the current and new statuses
                    for country_name, new_status in updates:
                        cursor.execute(
                            "SELECT [UK_FINANCIAL_SANCTIONS] FROM TblSanctionsMap WHERE [COUNTRY_NAME_ENG] = ?",
                            country_name
                        )
                        result = cursor.fetchone()
                        if result:
                            old_status = result[0] if result[0] is not None else 'NO'
                            if old_status.lower() != new_status.lower():
                                changes.append((country_name, old_status, new_status))
                                logging.info(
                                    f"Change detected: Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")
        except pyodbc.Error as e:
            logging.error(f"Database error during change detection: {e}")
        except Exception as e:
            logging.error(f"General error during change detection: {e}")
        return changes


def main():
    db_name = 'AXIOM_PARIS'
    sanctions_url = 'https://www.gov.uk/government/collections/financial-sanctions-regime-specific-consolidated-lists-and-releases'

    updater = UKSanctionsUpdater(db_name)

    # Parse the sanctions URL to get the sanctioned countries
    sanctioned_countries = updater.parse_financial_sanctions(sanctions_url)
    if sanctioned_countries:
        logging.info(f"Sanctioned countries found: {', '.join(sanctioned_countries)}")

        # Collect updates from the database
        updates = updater.collect_updates(sanctioned_countries)

        # Update the database based on the updates
        logging.info("Updating the database with new UK sanctions data...")
        updater.update_database_UKsanctions(updates)

        # Check for changes in the database
        changes = updater.check_database_changes_UKsanctions(updates)
        if changes:
            logging.info("Changes detected in the database:")
            for change in changes:
                logging.info(f"Country: {change[0]}, Old Status: {change[1]}, New Status: {change[2]}")
    else:
        logging.error("No sanctioned countries found or failed to parse the HTML content.")

if __name__ == "__main__":
    main()
