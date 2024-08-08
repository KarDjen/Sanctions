import os
import re
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EUFATFUpdater:
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

            # Find the table with the high-risk countries
            table = soup.find('table', {'class': 'ecl-table'})
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if cols:
                        country_name = cols[0].text.strip().upper()
                        # Mapping countries with specific names
                        if country_name == 'MYANMAR':
                            country_name = 'MYANMAR (BURMA)'
                        elif country_name == 'NORTH KOREA':
                            country_name = 'DEMOCRATIC PEOPLE\'S REPUBLIC OF KOREA (DPRK - NORTH KOREA)'
                        countries.append(unidecode(country_name))

            return countries
        return None

    def update_database_EUFATF(self, updates):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Ensure column can store the values properly
            cursor.execute("""
                ALTER TABLE TblCountries_New
                ALTER COLUMN [EU_-_AML_HIGH-RISK_COUNTRIES] NVARCHAR(50)
            """)
            cnx.commit()

            # Set all countries to "NO" initially
            cursor.execute("UPDATE TblCountries_New SET [EU_-_AML_HIGH-RISK_COUNTRIES] = 'NO'")
            cnx.commit()

            # Update only the high-risk countries to "YES"
            for country_name, high_risk_status in updates:
                update_query = f"""
                    UPDATE TblCountries_New
                    SET [EU_-_AML_HIGH-RISK_COUNTRIES] = ?
                    WHERE [COUNTRY_NAME_(ENG)] = ?
                """
                cursor.execute(update_query, high_risk_status, country_name)
                logging.info(f"Updated {country_name} to {high_risk_status}")

            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

    def check_database_changes_EUFATF(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, new_high_risk_status in updates:
                cursor.execute(
                    "SELECT [EU_-_AML_HIGH-RISK_COUNTRIES] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?",
                    country_name)
                result = cursor.fetchone()
                if result:
                    old_high_risk_status = result[0] if result[0] is not None else 'NO'
                    if old_high_risk_status != new_high_risk_status:
                        changes.append((country_name, old_high_risk_status, new_high_risk_status))
                        if new_high_risk_status.upper() == 'YES':
                            logging.info(
                                f"Country: {country_name}, Old Status: {old_high_risk_status}, New Status: {new_high_risk_status}")
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

def main():
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'
    html_url = 'https://finance.ec.europa.eu/financial-crime/anti-money-laundering-and-countering-financing-terrorism-international-level_en'

    updater = EUFATFUpdater(db_name)

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
