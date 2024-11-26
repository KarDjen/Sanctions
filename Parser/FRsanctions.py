"""
This script is used to update the sanctions columns for France in the TblSanctionsMap table.
It scrapes the French Treasury website to get the latest sanctions information for each country.
It then updates the database with the new information and logs any changes.
"""

import re
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pyodbc
import logging
from Logic.ComputedLogic import get_sanctions_map_columns_sql

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Class to handle the French sanctions updates
class FRSanctionsUpdater:

    # Initialize the updater with the database name
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
            re.compile(r'gel[s]? des avoirs|gels d\'avoirs', re.IGNORECASE): ('Asset Freezes', '[FR_ASSET_FREEEZE]'),
            re.compile(r'embargo[s]? sectoriel[s]?', re.IGNORECASE): ('Sectoral Embargoes', '[FR_SECTORAL_EMBARGO]'),
            re.compile(r'embargo[s]? militaire[s]?', re.IGNORECASE): ('Military Embargoes', '[FR_MILITARY_EMBARGO]'),
            re.compile(r'embargos sectoriel[s]? et militaire[s]?', re.IGNORECASE): ('Sectoral and Military Embargoes', ['[FR_SECTORAL_EMBARGO]', '[FR_MILITARY_EMBARGO]']),
            re.compile(r'equipements (de )?repression interne', re.IGNORECASE): ('Internal Repression Equipment', '[FR_INTERNAL_REPRESSION_EQUIPMENT]'),
            re.compile(r'(?<!\s)repression interne', re.IGNORECASE): ('Internal Repression', '[FR_INTERNAL_REPRESSION]'),
            re.compile(r'restrictions sectorielles', re.IGNORECASE): ('Sectoral Restrictions', '[FR_SECTORAL_RESTRICTIONS]'),
            re.compile(r'restrictions financi[eè]res', re.IGNORECASE): ('Financial Restrictions', '[FR_FINANCIAL_RESTRICTIONS]'),
            re.compile(r'interdiction[s]? de voyager', re.IGNORECASE): ('Travel Bans', '[FR_TRAVEL_BANS]'),
        }

    # Parse the main URL to get the country URLs
    def parse_main_url(self, main_url):
        response = requests.get(main_url)
        parsed_country_urls = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Find the section with the country URLs
            section = soup.find('h2', string=re.compile(r"1\. Vous voulez connaître les régimes de sanctions en vigueur")).find_next('p')
            if section:
                country_links = section.find_all('a', href=True)
                for link in country_links:
                    parsed_country_urls.append(link['href'])
        return parsed_country_urls

    # Parse the country URL to get the sanctions information
    def parse_country_url(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            sections = soup.find_all('section', class_='page-section')
            return sections
        return None

    # Collect the updates for the sanctions columns
    def collect_updates(self):
        updates = []
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            all_country_names = {unidecode(row[0].strip().upper()): row[0] for row in cursor.execute("SELECT [COUNTRY_NAME_FR] FROM TblSanctionsMap").fetchall()}
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

    # Update the database with the new sanctions information
    def update_database_FRsanctions(self, updates):
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:

                    # Lists to track changes from YES to NO and NO to YES
                    changes_yes_to_no = []
                    changes_no_to_yes = []

                    # Step 1: Drop dependent computed columns
                    logging.info("Dropping dependent computed columns...")
                    cursor.execute("""
                        IF EXISTS (SELECT 1 
                                   FROM sys.columns 
                                   WHERE name IN ('LEVEL_OF_RISK', 'LEVEL_OF_VIGILANCE', 'LIST') 
                                   AND object_id = OBJECT_ID('TblSanctionsMap'))
                        BEGIN
                            ALTER TABLE TblSanctionsMap 
                            DROP COLUMN LEVEL_OF_RISK, LEVEL_OF_VIGILANCE, LIST;
                        END
                    """)
                    cnx.commit()
                    logging.info("Dropped dependent computed columns successfully.")

                    # Step 2: Ensure column structure for storing the values
                    logging.info("Ensuring column structure is correct...")
                    for _, db_column, _ in updates:
                        cursor.execute(f"""
                            ALTER TABLE TblSanctionsMap
                            ALTER COLUMN {db_column} NVARCHAR(50)
                        """)
                    cnx.commit()
                    logging.info("Column structure updated successfully.")

                    # Step 3: Track current status before updates for comparison
                    country_status_before = {}
                    cursor.execute(
                        "SELECT [COUNTRY_NAME_FR], [FR_ASSET_FREEEZE], [FR_SECTORAL_EMBARGO], [FR_MILITARY_EMBARGO] FROM TblSanctionsMap")
                    for row in cursor.fetchall():
                        country_status_before[row[0]] = {  # Keep track of all columns you are updating
                            'FR_ASSET_FREEEZE': row[1],
                            'FR_SECTORAL_EMBARGO': row[2],
                            'FR_MILITARY_EMBARGO': row[3]
                        }

                    # Step 4: First update sanctions based on parsed list (set 'YES' or 'NO')
                    logging.info("Updating sanctions data based on parsed list...")
                    if updates:
                        for country_name, db_column, status in updates:
                            cursor.execute(f"""
                                UPDATE TblSanctionsMap
                                SET {db_column} = ?
                                WHERE [COUNTRY_NAME_FR] = ?
                            """, status, country_name)
                            logging.info(f"Updated {country_name} to '{status}' for {db_column}")
                        cnx.commit()

                    # Step 5: Track changes from NO to YES and YES to NO
                    cursor.execute(
                        "SELECT [COUNTRY_NAME_FR], [FR_ASSET_FREEEZE], [FR_SECTORAL_EMBARGO], [FR_MILITARY_EMBARGO] FROM TblSanctionsMap")
                    country_status_after = {}
                    for row in cursor.fetchall():
                        country_status_after[row[0]] = {
                            'FR_ASSET_FREEEZE': row[1],
                            'FR_SECTORAL_EMBARGO': row[2],
                            'FR_MILITARY_EMBARGO': row[3]
                        }

                    # Compare before and after states
                    for country, before_columns in country_status_before.items():
                        after_columns = country_status_after.get(country)
                        if after_columns:
                            for column, before_value in before_columns.items():
                                after_value = after_columns.get(column)
                                if before_value == 'YES' and after_value == 'NO':
                                    changes_yes_to_no.append((country, column))
                                elif before_value == 'NO' and after_value == 'YES':
                                    changes_no_to_yes.append((country, column))

                    # Step 6: Set remaining countries (not in the update list) to 'NO'
                    logging.info("Setting remaining countries to 'NO' for each column not already updated...")

                    updated_countries = {country_name for country_name, _, _ in updates}

                    # Set all other countries to 'NO' for each column
                    for _, db_column, _ in updates:
                        cursor.execute(f"""
                            UPDATE TblSanctionsMap
                            SET {db_column} = 'NO'
                            WHERE [COUNTRY_NAME_FR] NOT IN ({','.join(['?'] * len(updated_countries))})
                        """, tuple(updated_countries))
                        logging.info(f"Set remaining countries to 'NO' for {db_column}.")
                        cnx.commit()

                    # Step 7: Recreate computed columns
                    logging.info("Recreating computed columns...")
                    cursor.execute(get_sanctions_map_columns_sql())
                    cnx.commit()
                    logging.info("Recreated computed columns successfully.")

                    # Step 8: Log the changes
                    if changes_yes_to_no:
                        logging.info("Countries switched from YES to NO:")
                        for country, column in changes_yes_to_no:
                            logging.info(f"Country: {country}, Column: {column}, Change: YES -> NO")
                    if changes_no_to_yes:
                        logging.info("Countries switched from NO to YES:")
                        for country, column in changes_no_to_yes:
                            logging.info(f"Country: {country}, Column: {column}, Change: NO -> YES")

        except pyodbc.Error as e:
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
                cursor.execute(f"SELECT {db_column} FROM TblSanctionsMap WHERE [COUNTRY_NAME_FR] = ?", country_name)
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
    # Database name
    db_name = 'AXIOM_PARIS'

    # Initialize the updater
    updater = FRSanctionsUpdater(db_name)

    try:
        # Collect updates for FR sanctions
        updates = updater.collect_updates()

        # Update database for FR sanctions
        updater.update_database_FRsanctions(updates)

        # Check and report changes
        changes = updater.check_database_changes_FRsanctions(updates)
        if changes:
            logging.info("Changes detected in the database:")
            for change in changes:
                logging.info(f"Country: {change[0]}, Column: {change[1]}, Old Status: {change[2]}, New Status: {change[3]}")

    except Exception as e:
        logging.error(f"Error during update: {e}")


if __name__ == "__main__":
    main()
