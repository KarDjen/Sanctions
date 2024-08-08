import re
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class FRSanctionsUpdater:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )
        self.measures_dict = {
            re.compile(r'gel[s]? des avoirs|gels d\'avoirs', re.IGNORECASE): (
                'Asset Freezes', '[FR__-_ASSET_FREEEZE]'),
            re.compile(r'embargo[s]? sectoriel[s]?', re.IGNORECASE): (
                'Sectoral Embargoes', '[FR_-_SECTORAL_EMBARGO]'),
            re.compile(r'embargo[s]? militaire[s]?', re.IGNORECASE): (
                'Military Embargoes', '[FR_-_MILITARY__EMBARGO]'),
            re.compile(r'embargos sectoriel[s]? et militaire[s]?', re.IGNORECASE): (
                'Sectoral and Military Embargoes', ['[FR_-_SECTORAL_EMBARGO]', '[FR_-_MILITARY__EMBARGO]']),
            re.compile(r'equipements (de )?repression interne', re.IGNORECASE): (
                'Internal Repression Equipment', '[FR_-_INTERNAL_REPRESSION_EQUIPMENT]'),
            re.compile(r'(?<!\s)repression interne', re.IGNORECASE): (
                'Internal Repression', '[FR_-_INTERNAL_REPRESSION]'),
            re.compile(r'restrictions sectorielles', re.IGNORECASE): (
                'Sectoral Restrictions', '[FR_-_SECTORAL_RESTRICTIONS]'),
            re.compile(r'restrictions financi[eè]res', re.IGNORECASE): (
                'Financial Restrictions', '[FR_-_FINANCIAL_RESTRICTIONS]'),
            re.compile(r'interdiction[s]? de voyager', re.IGNORECASE): ('Travel Bans', '[FR_-_TRAVEL_BANS]'),
        }

    def update_column_lengths(self):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            alter_table_commands = [
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR__-_ASSET_FREEEZE] VARCHAR(3);",
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR_-_SECTORAL_EMBARGO] VARCHAR(3);",
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR_-_MILITARY__EMBARGO] VARCHAR(3);",
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR_-_INTERNAL_REPRESSION_EQUIPMENT] VARCHAR(3);",
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR_-_INTERNAL_REPRESSION] VARCHAR(3);",
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR_-_SECTORAL_RESTRICTIONS] VARCHAR(3);",
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR_-_FINANCIAL_RESTRICTIONS] VARCHAR(3);",
                "ALTER TABLE TblCountries_New ALTER COLUMN [FR_-_TRAVEL_BANS] VARCHAR(3);",
            ]
            for command in alter_table_commands:
                cursor.execute(command)
            cnx.commit()
            logging.info("Column lengths updated successfully.")
        except Exception as e:
            logging.error(f"Error updating column lengths: {e}")
        finally:
            cursor.close()
            cnx.close()

    def initialize_columns_to_no(self):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            update_commands = [
                "UPDATE TblCountries_New SET [FR__-_ASSET_FREEEZE] = 'NO';",
                "UPDATE TblCountries_New SET [FR_-_SECTORAL_EMBARGO] = 'NO';",
                "UPDATE TblCountries_New SET [FR_-_MILITARY__EMBARGO] = 'NO';",
                "UPDATE TblCountries_New SET [FR_-_INTERNAL_REPRESSION_EQUIPMENT] = 'NO';",
                "UPDATE TblCountries_New SET [FR_-_INTERNAL_REPRESSION] = 'NO';",
                "UPDATE TblCountries_New SET [FR_-_SECTORAL_RESTRICTIONS] = 'NO';",
                "UPDATE TblCountries_New SET [FR_-_FINANCIAL_RESTRICTIONS] = 'NO';",
                "UPDATE TblCountries_New SET [FR_-_TRAVEL_BANS] = 'NO';",
            ]
            for command in update_commands:
                cursor.execute(command)
            cnx.commit()
            logging.info("All columns initialized to 'NO' for all countries.")
        except Exception as e:
            logging.error(f"Error initializing columns to 'NO': {e}")
        finally:
            cursor.close()
            cnx.close()

    def parse_main_url(self, main_url):
        response = requests.get(main_url)
        parsed_country_urls = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            section = soup.find('h2', string=re.compile(r"1\. Vous voulez connaître les régimes de sanctions en vigueur")).find_next('p')
            if section:
                country_links = section.find_all('a', href=True)
                for link in country_links:
                    parsed_country_urls.append(link['href'])
        return parsed_country_urls

    def parse_country_url(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            sections = soup.find_all('section', class_='page-section')
            return sections
        return None

    def collect_updates(self):
        updates = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            all_country_names = {unidecode(row[0].strip().upper()): row[0] for row in cursor.execute("SELECT [COUNTRY_NAME_(FR)] FROM TblCountries_New").fetchall()}
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error retrieving country names from database: {e}")
            return updates

        parsed_country_urls = self.parse_main_url('https://www.tresor.economie.gouv.fr/services-aux-entreprises/sanctions-economiques')
        logging.info(f"Found URLs: {parsed_country_urls}")

        for country_name, db_country_name in all_country_names.items():
            # Special case handling for Russia
            if country_name == "RUSSIE":
                url_country_name = "russie-en-lien-avec-la-violation-par-la-russie-de-la-souverainete-et-de-l-integrite-territoriale-de-l-ukraine"
            else:
                url_country_name = unidecode(country_name.replace(' ', '-')).lower()

            country_url = f'https://www.tresor.economie.gouv.fr/services-aux-entreprises/sanctions-economiques/{url_country_name}'
            logging.info(f"Trying URL: {country_url}")

            if country_url in parsed_country_urls:
                sections = self.parse_country_url(country_url)

                if sections:
                    country_updates = {db_column: 'NO' for _, db_column in self.measures_dict.values() if not isinstance(db_column, list)}
                    for db_column_list in [db_column for _, db_column in self.measures_dict.values() if isinstance(db_column, list)]:
                        for db_column in db_column_list:
                            country_updates[db_column] = 'NO'

                    for section in sections:
                        headings = section.find_all(
                            lambda tag: tag.name.startswith('h') and tag.text and 'Mesures restrictives' in tag.text)
                        for heading in headings:
                            ul_tag = heading.find_next_sibling('ul')
                            if ul_tag:
                                li_tags = ul_tag.find_all('li')
                                measures = [unidecode(li.text.strip()) for li in li_tags]
                                for pattern, (_, db_column) in self.measures_dict.items():
                                    found_sanction = any(re.search(pattern, measure) for measure in measures)
                                    if found_sanction:
                                        if isinstance(db_column, list):
                                            for col in db_column:
                                                country_updates[col] = 'YES'
                                        else:
                                            country_updates[db_column] = 'YES'
                    # Log mapped countries and measures
                    for db_column, status in country_updates.items():
                        updates.append((db_country_name, db_column, status))
                        if status == 'YES':
                            logging.info(f"Parsed {db_country_name}: {db_column} set to YES")

        logging.info(f"Collected {len(updates)} updates.")
        return updates

    def update_database_FRsanctions(self, updates):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            for country_name, db_column, new_status in updates:
                cursor.execute(f"SELECT {db_column} FROM TblCountries_New WHERE [COUNTRY_NAME_(FR)] = ?", country_name)
                result = cursor.fetchone()
                if result:
                    old_status = result[0]
                    if old_status != new_status:
                        update_query = f"""
                            UPDATE TblCountries_New
                            SET {db_column} = ?
                            WHERE [COUNTRY_NAME_(FR)] = ?
                        """
                        cursor.execute(update_query, new_status, country_name)
                        logging.info(f"Updated {country_name} - {db_column} from {old_status} to {new_status}")

            cnx.commit()
            logging.info("Database updated successfully.")
        except Exception as e:
            logging.error(f"Error updating SQL database: {e}")
        finally:
            cursor.close()
            cnx.close()

    def check_database_changes_FRsanctions(self, updates):
        changes = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            for country_name, db_column, new_status in updates:
                cursor.execute(f"SELECT {db_column} FROM TblCountries_New WHERE [COUNTRY_NAME_(FR)] = ?", country_name)
                result = cursor.fetchone()
                if result:
                    old_status = result[0]
                    if old_status != new_status:
                        changes.append((country_name, db_column, old_status, new_status))
                        logging.info(f"Country: {country_name}, Column: {db_column}, Old Status: {old_status}, New Status: {new_status}")
            logging.info("Database changes checked.")
        except Exception as e:
            logging.error(f"Error checking database changes: {e}")
        finally:
            cursor.close()
            cnx.close()
        return changes


def main():
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'

    updater = FRSanctionsUpdater(db_name)

    try:
        # Update column lengths
        updater.update_column_lengths()

        # Initialize all relevant columns to "NO"
        updater.initialize_columns_to_no()

        # Collect updates for FR sanctions
        updates = updater.collect_updates()

        # Update database for FR sanctions
        updater.update_database_FRsanctions(updates)

        # Check and report changes
        changes = updater.check_database_changes_FRsanctions(updates)
        if changes:
            logging.info("Changes detected in the database:")
            for change in changes:
                logging.info(
                    f"Country: {change[0]}, Column: {change[1]}, Old Status: {change[2]}, New Status: {change[3]}")

    except Exception as e:
        logging.error(f"Error during update: {e}")


if __name__ == "__main__":
    main()
