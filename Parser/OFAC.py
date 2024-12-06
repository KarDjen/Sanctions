import os

import dotenv
import requests
import csv
from unidecode import unidecode
import pyodbc
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import re
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


class OFACUpdater:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={uid};'
            f'PWD={pwd}'
        )

    def normalize_country_name(self, name):
        normalized_name = unidecode(name.strip().upper().replace(' ', ''))
        return self.map_special_countries(normalized_name)

    def map_special_countries(self, name):
        # Add specific mappings for country names
        country_mappings = {
            "DPRK": "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA (DPRK - NORTH KOREA)",
            "BURMA": "MYANMAR (BURMA)"
        }
        return country_mappings.get(name, name)

    def parse_csv(self, url):
        session = requests.Session()
        retry = Retry(total=5, read=5, connect=5, backoff_factor=0.3, status_forcelist=(500, 502, 504))
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        try:
            response = session.get(url)
            response.raise_for_status()
            decoded_content = response.content.decode('utf-8')
            csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
            next(csv_reader)  # Skip header row
            countries = set()

            for row in csv_reader:
                if len(row) > 11:
                    cell_content = row[11].strip().upper()
                    if cell_content:
                        country_names = re.split(r'[;\s]\s*', cell_content)
                        for name in country_names:
                            normalized_country = self.normalize_country_name(name)
                            if normalized_country:
                                countries.add(normalized_country)

            return countries
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching CSV file from {url}: {e}")
            return None

    def collect_updates(self, csv_url):
        return self.parse_csv(csv_url)

    def update_database_OFAC(self, csv_countries):
        try:
            # Connect to the database
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    # Drop dependent computed columns before modifying the data
                    logging.info("Dropping dependent computed columns (LEVEL_OF_RISK, LEVEL_OF_VIGILANCE, LIST)...")
                    cursor.execute("""
                        IF EXISTS (SELECT 1 FROM sys.columns WHERE name IN ('LEVEL_OF_RISK', 'LEVEL_OF_VIGILANCE', 'LIST') 
                        AND object_id = OBJECT_ID('TblSanctionsMap'))
                        BEGIN
                            ALTER TABLE TblSanctionsMap 
                            DROP COLUMN LEVEL_OF_RISK, LEVEL_OF_VIGILANCE, LIST;
                        END
                    """)
                    cnx.commit()

                    # Ensure the column can store the values properly
                    logging.info("Ensuring column structure is correct...")
                    cursor.execute("""
                        ALTER TABLE TblSanctionsMap
                        ALTER COLUMN [US_OFAC_SANCTIONS] NVARCHAR(50)
                    """)
                    cnx.commit()

                    # Step 1: Set all countries to 'NO'
                    logging.info("Setting all countries to 'NO'...")
                    cursor.execute("""
                        UPDATE TblSanctionsMap
                        SET [US_OFAC_SANCTIONS] = 'NO'
                    """)
                    cnx.commit()

                    # Step 2: Update only the parsed countries to 'YES'
                    csv_countries_list = list(csv_countries)
                    max_params = 2000  # Stay within SQL Server's parameter limit

                    logging.info(f"Updating {len(csv_countries_list)} countries to 'YES' in batches...")

                    for i in range(0, len(csv_countries_list), max_params):
                        batch = csv_countries_list[i:i + max_params]
                        placeholders = ', '.join(['?'] * len(batch))
                        update_yes_query = f"""
                            UPDATE TblSanctionsMap
                            SET [US_OFAC_SANCTIONS] = 'YES'
                            WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') IN ({placeholders})
                        """
                        cursor.execute(update_yes_query, tuple(batch))
                        cnx.commit()

                        logging.info(f"Updated {len(batch)} countries to 'YES' in this batch.")

                    # Step 3: Recreate computed columns (LEVEL_OF_RISK, LEVEL_OF_VIGILANCE, LIST)
                    logging.info("Recreating computed columns (LEVEL_OF_RISK, LEVEL_OF_VIGILANCE, LIST)...")
                    cursor.execute(get_sanctions_map_columns_sql())
                    cnx.commit()

                    logging.info("Computed columns recreated successfully.")

        except pyodbc.Error as e:
            logging.error(f"Database error during OFAC updates: {e}")
        except Exception as e:
            logging.error(f"General error during OFAC updates: {e}")

    def get_summary_of_yes_countries(self):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute("""
                SELECT [COUNTRY_NAME_ENG] 
                FROM TblSanctionsMap 
                WHERE [US_OFAC_SANCTIONS] = 'YES'
            """)
            yes_countries = cursor.fetchall()
            yes_countries = [row[0] for row in yes_countries]
            cursor.close()
            cnx.close()

            return yes_countries

        except Exception as e:
            logging.error(f"Error fetching summary of 'YES' countries: {e}")
            return []

def main():
    # Database name and CSV URL

    csv_url = 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV'

    # Initialize the OFACUpdater
    updater = OFACUpdater(database)

    # Collect updates for OFAC
    csv_countries = updater.collect_updates(csv_url)
    if csv_countries:
        logging.info(f"\nOFAC sanctioned countries found: {', '.join(csv_countries)}")

        # Update the database based on the OFAC countries
        logging.info("Updating the database with new OFAC data...")
        updater.update_database_OFAC(csv_countries)

        # Get a summary of countries with 'YES' status
        yes_countries = updater.get_summary_of_yes_countries()
        logging.info(f"\nSummary of countries with 'YES' status in OFAC Sanction Program: {', '.join(yes_countries)}")
    else:
        logging.error("No OFAC sanctioned countries found or failed to parse the CSV content.")

if __name__ == "__main__":
    main()
