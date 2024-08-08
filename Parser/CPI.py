import os
import re
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CPIUpdater:
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

    def get_countries_from_database(self):
        countries = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute("SELECT [COUNTRY_NAME_(ENG)] FROM TblCountries_New")
            for row in cursor.fetchall():
                countries.append(unidecode(row[0].strip().upper()))
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error fetching countries from database: {e}")
        return countries

    def get_current_data_from_database(self, country_name):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute("SELECT [CPI_SCORE], [CPI_RANK] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?", country_name)
            row = cursor.fetchone()
            cursor.close()
            cnx.close()
            if row:
                return row[0], row[1]  # score, rank
            else:
                return None, None
        except Exception as e:
            logging.error(f"Error fetching current data from database: {e}")
            return None, None

    def format_country_name(self, country_name):
        country_name = re.sub(r'\(.*?\)', '', country_name).strip()
        formatted_country_name = unidecode(country_name.lower().replace(' ', '-'))
        return formatted_country_name

    def is_valid_number(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def parse_country_details(self, country_name):
        formatted_country_name = self.format_country_name(country_name)
        url = f'https://www.transparency.org/en/countries/{formatted_country_name}'
        response = requests.get(url)

        if response.status_code == 404:
            return 'N/A', 'N/A'

        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        score_tag = soup.find('dt', string='Score')
        score = score_tag.find_next('dd').get_text(strip=True).split('/')[0] if score_tag else 'N/A'

        rank_tag = soup.find('dt', string='Rank')
        rank = rank_tag.find_next('dd').get_text(strip=True).split('/')[0] if rank_tag else 'N/A'

        if not self.is_valid_number(score):
            score = 'N/A'
        if not self.is_valid_number(rank):
            rank = 'N/A'

        return score, rank

    def fetch_and_compare_country_details(self, country_name):
        score, rank = self.parse_country_details(country_name)
        if score == 'N/A':
            score = None
        else:
            score = int(float(score))  # Ensure the score is an integer
        if rank == 'N/A':
            rank = None
        else:
            rank = int(rank)  # Ensure the rank is an integer

        current_score, current_rank = self.get_current_data_from_database(country_name)
        changes = []
        if current_score != score:
            changes.append((country_name, 'CPI_SCORE', current_score, score))
        if current_rank != rank:
            changes.append((country_name, 'CPI_RANK', current_rank, rank))
        self.changes.extend(changes)

        return (country_name, score, rank)

    def update_database_CPI(self, countries):
        updates = []
        try:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self.fetch_and_compare_country_details, country): country for country in countries}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        updates.append(result)
                    except Exception as e:
                        logging.error(f"Error fetching and comparing country details: {e}")

            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            for update in updates:
                country_name, score, rank = update
                update_query = """
                    UPDATE TblCountries_New
                    SET [CPI_SCORE] = ?, [CPI_RANK] = ?
                    WHERE [COUNTRY_NAME_(ENG)] = ?
                """
                cursor.execute(update_query, score, rank, country_name)
                logging.info(f"Updated {country_name} with CPI Score: {score}, Rank: {rank}")

            cnx.commit()
            cursor.close()
            cnx.close()

        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")

        self.updates = updates
        return updates

    def check_database_changes_CPI(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, new_score, new_rank in updates:
                cursor.execute("SELECT [CPI_SCORE], [CPI_RANK] FROM TblCountries_New WHERE [COUNTRY_NAME_(ENG)] = ?",
                               country_name)
                result = cursor.fetchone()
                if result:
                    old_score, old_rank = result
                    if old_score != new_score:
                        changes.append((country_name, 'CPI_SCORE', old_score, new_score))
                    if old_rank != new_rank:
                        changes.append((country_name, 'CPI_RANK', old_rank, new_rank))
                        if new_rank.upper() == 'YES':
                            logging.info(f"Country: {country_name}, Old Rank: {old_rank}, New Rank: {new_rank}")
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

    def collect_updates(self):
        return self.updates

    def collect_changes(self):
        return self.changes


def main():
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'

    updater = CPIUpdater(db_name)

    # Fetch countries from the database
    countries = updater.get_countries_from_database()
    if not countries:
        logging.info("No countries found in the database.")
        return

    # Update the database with new CPI data
    logging.info("Updating the database with new CPI data...")
    updates = updater.update_database_CPI(countries)

    # Print collected updates and changes
    updates = updater.collect_updates()
    changes = updater.check_database_changes_CPI(updates)

    logging.info("\nCollected Updates:")
    for update in updates:
        logging.info(update)

    logging.info("\nCollected Changes:")
    for change in changes:
        logging.info(change)


if __name__ == "__main__":
    main()
