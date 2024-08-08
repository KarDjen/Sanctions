"""
Ready
"""

import os
import re
import requests
from bs4 import BeautifulSoup
import pyodbc
import logging
from unidecode import unidecode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FRTaxUpdater:
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

    def parse_html(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            countries = []

            table_header = soup.find('th', string=re.compile(r'Liste source', re.IGNORECASE))
            if table_header:
                table_body = table_header.find_next('tbody')
                if table_body:
                    rows = table_body.find_all('tr')
                    for row in rows:
                        columns = row.find_all('td')
                        if columns:
                            country_name = columns[0].text.strip().upper()
                            countries.append(unidecode(country_name))

            return countries
        return None

    def collect_updates(self, countries):
        updates = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute("SELECT [COUNTRY_NAME_(FR)] FROM TblCountries_New")
            all_countries = cursor.fetchall()

            for country_row in all_countries:
                country_name = unidecode(country_row[0].strip().upper())
                status = 'YES' if country_name in countries else 'NO'
                updates.append((country_name, status))

            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error collecting updates: {e}")
        return updates

    def update_database_FRtax(self, updates):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Ensure column can store the values properly
            cursor.execute("""
                ALTER TABLE TblCountries_New
                ALTER COLUMN [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] NVARCHAR(50)
            """)
            cnx.commit()

            # Set all countries to "NO" initially
            cursor.execute("UPDATE TblCountries_New SET [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO'")
            cnx.commit()

            for country_name, status in updates:
                update_query = f"""
                    UPDATE TblCountries_New
                    SET [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = ?
                    WHERE [COUNTRY_NAME_(FR)] = ?
                """
                cursor.execute(update_query, status, country_name)
                logging.info(f"Updated {country_name} to {status}")

            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

    def check_database_changes_FRtax(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, new_status in updates:
                cursor.execute(
                    "SELECT [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] FROM TblCountries_New WHERE [COUNTRY_NAME_(FR)] = ?",
                    country_name)
                result = cursor.fetchone()
                if result:
                    old_status = result[0] if result[0] is not None else 'NO'
                    if old_status.lower() != new_status.lower():
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
    html_url = 'https://www.douane.gouv.fr/actualites/lcb-ft-liste-des-etats-et-territoires-non-cooperatifs-en-matiere-fiscale'

    updater = FRTaxUpdater(db_name)

    # Parse the HTML to get the non-cooperative jurisdictions
    countries = updater.parse_html(html_url)
    if countries:
        logging.info(f"\nNon-cooperative jurisdictions found: {', '.join(countries)}")

        # Collect updates from the database
        updates = updater.collect_updates(countries)

        # Update the database based on the updates
        logging.info("Updating the database with new FR tax data...")
        updater.update_database_FRtax(updates)

        # Check for changes in the database
        changes = updater.check_database_changes_FRtax(updates)
        if changes:
            logging.info("\nChanges detected in the database:")
            for change in changes:
                logging.info(f"Country: {change[0]}, Old Status: {change[1]}, New Status: {change[2]}")

    else:
        logging.error("No non-cooperative jurisdictions found or failed to parse the HTML content.")

if __name__ == "__main__":
    main()
