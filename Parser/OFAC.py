import os
import requests
import csv
from unidecode import unidecode
import pyodbc
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OFACUpdater:
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

    def parse_csv(self, url):
        session = requests.Session()
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

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
                    words = cell_content.split()
                    for word in words:
                        countries.add(unidecode(word))

        return countries

    def collect_updates(self, csv_url):
        return self.parse_csv(csv_url)

    def update_database_OFAC(self, csv_countries):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Ensure column can store the values properly
            cursor.execute("""
                ALTER TABLE TblCountries_New
                ALTER COLUMN [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] NVARCHAR(50)
            """)
            cnx.commit()

            # Set all countries to "NO" initially
            cursor.execute("UPDATE TblCountries_New SET [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] = 'NO'")
            cnx.commit()

            # Update the database based on CSV countries
            cursor.execute("SELECT [COUNTRY_NAME_(ENG)] FROM TblCountries_New")
            all_countries = cursor.fetchall()

            for country_row in all_countries:
                country_name = unidecode(country_row[0].strip().upper())
                status = 'YES' if any(country_name in csv_country for csv_country in csv_countries) else 'NO'

                update_query = """
                    UPDATE TblCountries_New
                    SET [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] = ?
                    WHERE [COUNTRY_NAME_(ENG)] = ?
                """
                cursor.execute(update_query, status, country_name)
                logging.info(f"Updated {country_name} to {status}")

            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

    def check_database_changes_OFAC(self, csv_countries):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute(
                "SELECT [COUNTRY_NAME_(ENG)], [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] FROM TblCountries_New")
            all_countries = cursor.fetchall()

            for country_row in all_countries:
                country_name = unidecode(country_row[0].strip().upper())
                old_status = country_row[1] if country_row[1] is not None else 'NO'
                new_status = 'YES' if any(country_name in csv_country for csv_country in csv_countries) else 'NO'
                if old_status != new_status:
                    changes.append((country_name, old_status, new_status))
                    if new_status.upper() == 'YES':
                        logging.info(f"Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

def main():
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'
    csv_url = 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONS_PRIM.CSV'

    updater = OFACUpdater(db_name)

    # Collect updates for OFAC
    csv_countries = updater.collect_updates(csv_url)
    if csv_countries:
        logging.info(f"\nOFAC sanctioned countries found: {', '.join(csv_countries)}")

        # Update the database based on the OFAC countries
        logging.info("Updating the database with new OFAC data...")
        updater.update_database_OFAC(csv_countries)

        # Check for changes in the database
        changes = updater.check_database_changes_OFAC(csv_countries)
        if changes:
            logging.info("\nChanges detected in the database:")
            for change in changes:
                logging.info(f"Country: {change[0]}, Old Status: {change[1]}, New Status: {change[2]}")

    else:
        logging.error("No OFAC sanctioned countries found or failed to parse the CSV content.")

if __name__ == "__main__":
    main()
