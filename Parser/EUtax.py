"""
Ready
"""

import os
import re
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EUTaxUpdater:
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

    def clean_country_name(self, name):
        cleaned_name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        return [unidecode(n.strip().upper()) for n in cleaned_name.split(',')]

    def map_country_name(self, country_name):
        mapping = {
            'RUSSIAN FEDERATION': 'RUSSIA'
        }
        return mapping.get(country_name, country_name)

    def parse_html(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            non_cooperative_countries = []
            under_way_countries = []

            non_cooperative_tag = soup.find('p', {'id': 'd1e39-2-1'})
            if non_cooperative_tag:
                following_siblings = non_cooperative_tag.find_all_next('p', {'class': 'oj-ti-grseq-1'})
                for sibling in following_siblings:
                    bold_tag = sibling.find('span', {'class': 'oj-bold'})
                    if bold_tag and 'State of play' in bold_tag.text:
                        break
                    if bold_tag:
                        countries = bold_tag.text.strip().upper().split(',')
                        for country in countries:
                            non_cooperative_countries.extend(self.clean_country_name(country))

            commit_tags = soup.find_all('p', {'class': 'oj-normal'})
            for commit_tag in commit_tags:
                bold_tag = commit_tag.find('span', {'class': 'oj-bold'})
                if bold_tag:
                    countries_text = bold_tag.text.strip().upper().replace(' AND ', ', ')
                    countries = countries_text.split(',')
                    for country in countries:
                        under_way_countries.extend(self.clean_country_name(country))

            non_cooperative_countries = [self.map_country_name(country) for country in non_cooperative_countries]
            under_way_countries = [self.map_country_name(country) for country in under_way_countries]

            return non_cooperative_countries, under_way_countries
        return None, None

    def update_database_EUtax(self, non_cooperative_countries, under_way_countries):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Ensure column can store the values properly
            cursor.execute("""
                ALTER TABLE TblCountries_New
                ALTER COLUMN [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] NVARCHAR(50)
            """)
            cnx.commit()

            # Set all countries to "NO" initially
            cursor.execute("UPDATE TblCountries_New SET [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO'")
            cnx.commit()

            # Fetch all country names from the database
            cursor.execute("SELECT [COUNTRY_NAME_(ENG)] FROM TblCountries_New")
            all_countries = {unidecode(row[0].strip().upper()): row[0] for row in cursor.fetchall()}

            updated_cells_summary = []

            # Update the database with new statuses
            for country_name, mapped_country_name in all_countries.items():
                mapped_country_name = self.map_country_name(mapped_country_name)
                if country_name in non_cooperative_countries:
                    new_status = 'YES'
                elif country_name in under_way_countries:
                    new_status = 'NO'
                else:
                    new_status = 'NO'

                current_status_query = """
                    SELECT [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] 
                    FROM TblCountries_New 
                    WHERE [COUNTRY_NAME_(ENG)] = ?
                """
                cursor.execute(current_status_query, mapped_country_name)
                result = cursor.fetchone()
                current_status = result[0] if result else 'NO'

                if current_status != new_status:
                    update_query = """
                        UPDATE TblCountries_New
                        SET [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = ?
                        WHERE [COUNTRY_NAME_(ENG)] = ?
                    """
                    cursor.execute(update_query, new_status, mapped_country_name)
                    updated_cells_summary.append(
                        f"{mapped_country_name}: Status updated from {current_status} to {new_status}"
                    )

            cnx.commit()
            cursor.close()
            cnx.close()

            logging.info("\nSummary of updated cells:")
            for cell_summary in updated_cells_summary:
                logging.info(cell_summary)

        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

    def collect_updates(self):
        return self.updates

    def check_database_changes_EUtax(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, new_status in updates:
                cursor.execute(
                    "SELECT [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?",
                    country_name
                )
                result = cursor.fetchone()
                if result:
                    old_status = result[0] if result[0] is not None else 'NO'
                    if old_status != new_status:
                        changes.append(
                            (country_name, 'EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS', old_status, new_status)
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
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'
    html_url = 'https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A52024XG01804'

    updater = EUTaxUpdater(db_name)

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

