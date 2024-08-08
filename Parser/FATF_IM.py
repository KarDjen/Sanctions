import os
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class FATFIMUpdater:
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

    def build_url(self):
        base_url = 'https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/increased-monitoring-{}.html'
        months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october',
                  'november', 'december']
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

            start_tag = soup.find('h6', class_='cmp-title__text', string='Country')
            if not start_tag:
                return None

            countries_div = start_tag.find_next('p')
            if not countries_div:
                return None

            countries_text = countries_div.get_text()
            countries_list = [unidecode(country.strip().upper()) for country in countries_text.split(', ')]

            # Mapping countries with specific names
            country_mapping = {
                'MYANMAR (BURMA)': 'MYANMAR',
                'DEMOCRATIC PEOPLE\'S REPUBLIC OF KOREA (DPRK - NORTH KOREA)': 'NORTH KOREA',
                'CROATIA, DEMOCRATIC REPUBLIC OF THE CONGO': ['CROATIA', 'DEMOCRATIC REPUBLIC OF THE CONGO']
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

    def update_database_FATF_IM(self, high_risk_countries):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Ensure column can store the values properly
            cursor.execute("""
                ALTER TABLE TblCountries_New
                ALTER COLUMN [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] NVARCHAR(50)
            """)
            cnx.commit()

            # Set all countries to "NO" initially
            cursor.execute("UPDATE TblCountries_New SET [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO'")
            cnx.commit()

            for country_name in high_risk_countries:
                status = 'YES'
                update_query = """
                    UPDATE TblCountries_New
                    SET [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = ?
                    WHERE [COUNTRY_NAME_(ENG)] = ?
                """
                cursor.execute(update_query, status, country_name)
                logging.info(f"Updated {country_name} to {status}")

            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

    def check_database_changes_FATF_IM(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, new_status in updates:
                cursor.execute(
                    "SELECT [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?",
                    country_name)
                result = cursor.fetchone()
                if result:
                    old_status = result[0] if result[0] is not None else 'NO'
                    if old_status != new_status:
                        changes.append(
                            (country_name, 'FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING', old_status, new_status))
                        if new_status.upper() == 'YES':
                            logging.info(f"Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes


def main():
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'
    updater = FATFIMUpdater(db_name)

    # Build URL for the latest available increased monitoring page
    url = updater.build_url()
    if url:
        # Parse the HTML to get the high-risk countries
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
