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

class UKSanctionsUpdater:
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
        return [unidecode(n.strip().upper().replace('’', "'")) for n in cleaned_name.split(',')]

    def map_country_name(self, country_name):
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

    def parse_financial_sanctions(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            sanctioned_countries = []

            items = soup.find_all('div', {'class': 'gem-c-document-list__item-title'})
            for item in items:
                if item.a and 'Financial sanctions' in item.a.text:
                    country_text = item.a.text.split('Financial sanctions,')[-1].strip()
                    sanctioned_countries.extend(self.clean_country_name(country_text))

            sanctioned_countries = [self.map_country_name(country) for country in sanctioned_countries]
            return sanctioned_countries
        return []

    def match_country_name(self, country_name, target_country):
        if country_name == "DEMOCRATIC PEOPLE’S REPUBLIC OF KOREA (DPRK - NORTH KOREA)":
            pattern = re.compile(r"DEMOCRATIC PEOPLE['’]S REPUBLIC OF KOREA", re.IGNORECASE)
            return bool(pattern.search(target_country))
        else:
            return country_name == target_country

    def collect_updates(self, sanctioned_countries):
        updates = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute("SELECT [COUNTRY_NAME_(ENG)] FROM TblCountries_New")
            all_countries = cursor.fetchall()

            for country_row in all_countries:
                country_name = self.map_country_name(unidecode(country_row[0].strip().upper().replace('’', "'")))
                status = 'YES' if any(self.match_country_name(sanctioned_country, country_name) for sanctioned_country in sanctioned_countries) else 'NO'
                updates.append((country_name, status))

            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error collecting updates: {e}")
        return updates

    def update_database_UKsanctions(self, updates):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Ensure column can store the values properly
            cursor.execute("""
                ALTER TABLE TblCountries_New
                ALTER COLUMN [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] NVARCHAR(50)
            """)
            cnx.commit()

            # Set all countries to "NO" initially
            cursor.execute("UPDATE TblCountries_New SET [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] = 'NO'")
            cnx.commit()

            for country_name, status in updates:
                update_query = f"""
                    UPDATE TblCountries_New
                    SET [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] = ?
                    WHERE [COUNTRY_NAME_(ENG)] = ?
                """
                cursor.execute(update_query, status, country_name)
                logging.info(f"Updated {country_name} to {status}")

            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

    def check_database_changes_UKsanctions(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, new_status in updates:
                cursor.execute(
                    "SELECT [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?",
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
    sanctions_url = 'https://www.gov.uk/government/collections/financial-sanctions-regime-specific-consolidated-lists-and-releases'

    updater = UKSanctionsUpdater(db_name)

    # Parse the sanctions URL to get the sanctioned countries
    sanctioned_countries = updater.parse_financial_sanctions(sanctions_url)
    if sanctioned_countries:
        logging.info(f"\nSanctioned countries found: {', '.join(sanctioned_countries)}")

        # Collect updates from the database
        updates = updater.collect_updates(sanctioned_countries)

        # Update the database based on the updates
        logging.info("Updating the database with new UK sanctions data...")
        updater.update_database_UKsanctions(updates)

        # Check for changes in the database
        changes = updater.check_database_changes_UKsanctions(updates)
        if changes:
            logging.info("\nChanges detected in the database:")
            for change in changes:
                logging.info(f"Country: {change[0]}, Old Status: {change[1]}, New Status: {change[2]}")

    else:
        logging.error("No sanctioned countries found or failed to parse the HTML content.")

if __name__ == "__main__":
    main()
