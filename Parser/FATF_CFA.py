import os
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FATFCFAUpdater:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )

    def build_url(self):
        base_url = 'https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/Call-for-action-{}.html'
        months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
        current_year = datetime.now().year

        for year in range(current_year, 2027):
            for month in months:
                url = base_url.format(f'{month}-{year}')
                response = requests.head(url)
                if response.status_code == 200:
                    logging.info(f"Found valid URL: {url}")
                    return url
        return None

    def parse_html(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            countries = []

            titles = soup.find_all('h3')
            for title in titles:
                b_tag = title.find('b')
                if b_tag:
                    country_name = b_tag.text.strip().upper()
                    # Mapping countries with specific names
                    if country_name == 'MYANMAR':
                        country_name = 'MYANMAR (BURMA)'
                    elif country_name == 'DEMOCRATIC PEOPLE\'S REPUBLIC OF KOREA (DPRK)':
                        country_name = 'DEMOCRATIC PEOPLE\'S REPUBLIC OF KOREA (DPRK - NORTH KOREA)'
                    elif country_name == 'CROATIA, DEMOCRATIC REPUBLIC OF THE CONGO':
                        countries.append('CROATIA')
                        countries.append('REPUBLIC DEMOCRATIC OF THE CONGO')
                        continue
                    countries.append(unidecode(country_name))

            return countries
        return None

    def update_database_FATF_CFA(self, high_risk_countries):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Ensure column can store the values properly
            cursor.execute("""
                ALTER TABLE TblCountries_New
                ALTER COLUMN [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] NVARCHAR(50)
            """)
            cnx.commit()

            # Set all countries to "NO" initially
            cursor.execute("UPDATE TblCountries_New SET [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO'")
            cnx.commit()

            for country in high_risk_countries:
                status = 'YES'

                update_query = """
                    UPDATE TblCountries_New
                    SET [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = ?
                    WHERE [COUNTRY_NAME_(ENG)] = ?
                """
                cursor.execute(update_query, status, country)
                logging.info(f"Updated {country} to {status}")

            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

    def check_database_changes_FATFCFA(self, high_risk_countries):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Fetch the current status from the database
            cursor.execute("SELECT [COUNTRY_NAME_(ENG)], [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] FROM TblCountries_New")
            all_countries = cursor.fetchall()

            # Create a set of high-risk countries for quick lookup
            high_risk_set = {unidecode(country.strip().upper()) for country in high_risk_countries}

            for country_row in all_countries:
                country_name = unidecode(country_row[0].strip().upper())
                old_status = country_row[1] if country_row[1] is not None else 'NO'
                new_status = 'YES' if country_name in high_risk_set else 'NO'

                if old_status != new_status:
                    changes.append((country_name, old_status, new_status))
                    if new_status == 'YES':
                        logging.info(f"Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")

            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

def main():
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'
    updater = FATFCFAUpdater(db_name)

    # Build URL for the latest available call for action page
    url = updater.build_url()
    if url:
        # Parse the HTML to get the high-risk countries
        high_risk_countries = updater.parse_html(url)
        if high_risk_countries:
            logging.info(f"High-risk countries found: {', '.join(high_risk_countries)}")

            # Check database changes
            logging.info("Checking for database changes...")
            changes = updater.check_database_changes_FATFCFA(high_risk_countries)
            if changes:
                logging.info(f"Changes detected: {changes}")

            # Update the database based on the high-risk countries
            logging.info("Updating the database with new FATF CFA data...")
            updater.update_database_FATF_CFA(high_risk_countries)
        else:
            logging.error("No high-risk countries found or failed to parse the HTML content.")
    else:
        logging.error("No valid URL found for call for action data.")

if __name__ == "__main__":
    main()
