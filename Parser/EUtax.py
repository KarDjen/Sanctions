"""
This script is used to parse the EU tax list and update the SQL database with the new data.
It also checks for changes in the database and logs the changes.
"""
import os
# Importing required libraries
import re

import dotenv
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging
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


# Class to handle EU tax list updates
class EUTaxUpdater:

    # Constructor to initialize the database connection and other variables
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

    # Function to clean and normalize country names
    def clean_country_name(self, name):
        # Remove parentheses and normalize country names
        cleaned_name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        return [unidecode(n.strip().upper().replace('’', "'")) for n in cleaned_name.split(',')]

    # Function to map country names to standard names
    def map_country_name(self, country_name):
        mapping = {
            'RUSSIAN FEDERATION': 'RUSSIA'
        }
        return mapping.get(country_name, country_name)

    def normalize_country_name(self, country_name):
        # Normalize country name by removing extra spaces, handling smart quotes, and applying unidecode
        country_name = unidecode(country_name.strip().upper())
        country_name = country_name.replace('’', "'")  # Replace smart quotes with standard quotes
        return country_name

    # Function to parse the HTML content and extract non-cooperative and under-way countries
    def parse_html(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser') # Parsing the HTML content
            non_cooperative_countries = [] # List to store non-cooperative countries
            under_way_countries = [] # List to store under-way countries

            # Parsing non-cooperative countries section
            non_cooperative_tag = soup.find('p', {'id': 'd1e39-2-1'})
            if non_cooperative_tag:
                following_siblings = non_cooperative_tag.find_all_next('p', {'class': 'oj-ti-grseq-1'}) # Finding following siblings
                for sibling in following_siblings:
                    bold_tag = sibling.find('span', {'class': 'oj-bold'})
                    if bold_tag and 'State of play' in bold_tag.text:
                        break
                    if bold_tag:
                        countries = bold_tag.text.strip().upper().split(',')
                        for country in countries:
                            non_cooperative_countries.extend(self.clean_country_name(country))

            # Parsing under-way countries section
            commit_tags = soup.find_all('p', {'class': 'oj-normal'})
            for commit_tag in commit_tags:
                bold_tag = commit_tag.find('span', {'class': 'oj-bold'})
                if bold_tag:
                    countries_text = bold_tag.text.strip().upper().replace(' AND ', ', ')
                    countries = countries_text.split(',')
                    for country in countries:
                        under_way_countries.extend(self.clean_country_name(country))

            # Normalize country names
            non_cooperative_countries = [self.map_country_name(country) for country in non_cooperative_countries]
            under_way_countries = [self.map_country_name(country) for country in under_way_countries]

            return non_cooperative_countries, under_way_countries
        return None, None

    # Function to update the database with the new EU tax data
    def update_database_EUtax(self, non_cooperative_countries, under_way_countries):
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:

                    # Drop dependent computed columns
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
                    logging.info("Dropped dependent computed columns successfully.")

                    # Ensure the column can store the values properly
                    logging.info("Altering column [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS]...")
                    cursor.execute("""
                        ALTER TABLE TblSanctionsMap
                        ALTER COLUMN [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] NVARCHAR(50)
                    """)
                    cnx.commit()

                    # Normalize country names
                    normalized_non_coop_countries = [self.normalize_country_name(country) for country in
                                                     non_cooperative_countries]
                    normalized_under_way_countries = [self.normalize_country_name(country) for country in
                                                      under_way_countries]
                    all_normalized_countries = normalized_non_coop_countries + normalized_under_way_countries

                    # Step 1: Set all countries to 'NO' first
                    logging.info("Setting all countries to 'NO'...")
                    cursor.execute("""
                        UPDATE TblSanctionsMap
                        SET [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO'
                    """)
                    cnx.commit()

                    # Step 2: Update non-cooperative countries to 'YES'
                    if normalized_non_coop_countries:
                        batch_size = 500  # Batch size for bulk updates
                        for i in range(0, len(normalized_non_coop_countries), batch_size):
                            batch = normalized_non_coop_countries[i:i + batch_size]
                            placeholders = ', '.join('?' for _ in batch)
                            logging.info(f"Bulk updating {len(batch)} non-cooperative countries to 'YES'...")
                            bulk_update_yes_query = f"""
                                UPDATE TblSanctionsMap
                                SET [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES'
                                WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') IN ({placeholders})
                            """
                            cursor.execute(bulk_update_yes_query, batch)
                            logging.info(f"Set the following non-cooperative countries to 'YES': {batch}")
                            cnx.commit()

                    # Step 3: Update countries under review to a special flag if needed (optional logic for under-way countries)
                    if normalized_under_way_countries:
                        # For demonstration purposes, if there is a separate handling for under_way_countries, add it here.
                        pass

                    # Step 4: Recreate the dropped computed columns
                    logging.info("Recreating dependent computed columns...")
                    cursor.execute(get_sanctions_map_columns_sql())
                    cnx.commit()
                    logging.info("Recreated computed columns successfully.")

        except pyodbc.Error as e:
            logging.error(f"Database error during EU tax updates: {e}")
        except Exception as e:
            logging.error(f"General error during EU tax updates: {e}")

    # Function to collect the updates made to the database
    def collect_updates(self):
        return self.updates

    # Function to check for changes in the database
    def check_database_changes_EUtax(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, new_status in updates:
                normalized_country_name = self.clean_country_name(country_name)[0]
                cursor.execute(
                    "SELECT [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] FROM TblSanctionsMap WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = ?",
                    normalized_country_name
                )
                result = cursor.fetchone()
                if result:
                    old_status = result[0] if result[0] is not None else 'NO'
                    if old_status != new_status:
                        changes.append(
                            (country_name, 'EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS', old_status, new_status)
                        )
                        if new_status.upper() == 'YES':
                            logging.info(f"Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

    def collect_changes(self):
        return self.changes


def main():

    html_url = 'https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A52024XG01804'

    updater = EUTaxUpdater(database)

    # Parse the HTML to get the non-cooperative and under-way countries
    non_cooperative_countries, under_way_countries = updater.parse_html(html_url)
    if non_cooperative_countries or under_way_countries:
        logging.info(f"\nNon-cooperative countries found: {', '.join(non_cooperative_countries)}")
        logging.info(f"\nUnder-way countries found: {', '.join(under_way_countries)}")

        # Update the database based on the parsed data
        logging.info("Updating the database with new EU tax data...")
        updater.update_database_EUtax(non_cooperative_countries, under_way_countries)

        # Check for changes in the database
        updates = updater.collect_updates()
        changes = updater.check_database_changes_EUtax(updates)
        if changes:
            logging.info("\nChanges detected in the database:")
            for change in changes:
                logging.info(f"Country: {change[0]}, Old Status: {change[2]}, New Status: {change[3]}")

    else:
        logging.info("No non-cooperative or under-way countries found or failed to parse the HTML content.")

if __name__ == "__main__":
    main()
