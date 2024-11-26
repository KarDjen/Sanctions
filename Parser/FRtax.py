"""
This script is used to update the 'FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS' column in the 'TblSanctionsMap' table.
The script fetches the list of non-cooperative jurisdictions from the French Customs website and updates the database accordingly.
It also checks for changes in the database and logs the changes.
"""


# Import the required libraries
import re
import requests
from bs4 import BeautifulSoup
import pyodbc
import logging
from unidecode import unidecode
from Logic.ComputedLogic import get_sanctions_map_columns_sql

# Set up the logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the class for updating the French tax list
class FRTaxUpdater:

    # Initialize the class with the database name and connection string
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

    # Parse the HTML content to extract the non-cooperative jurisdictions
    def parse_html(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            countries = []

            # Find the table header with the source list ("Liste source")
            table_header = soup.find('th', string=re.compile(r'Liste source', re.IGNORECASE))
            if table_header:
                table_body = table_header.find_next('tbody') # Find the table body after the header
                if table_body:
                    rows = table_body.find_all('tr') # Find all rows in the table body
                    for row in rows:
                        columns = row.find_all('td') # Find all columns in the row
                        if columns:
                            country_name = columns[0].text.strip().upper() # Extract the country name from the first column
                            countries.append(unidecode(country_name)) # Normalize the country name and add to the list
            return countries
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching HTML content from {url}: {e}")
            return None

    def normalize_country_name(self, country_name):
        # Normalize country name by removing extra spaces, handling smart quotes, and applying unidecode
        country_name = unidecode(country_name.strip().upper())
        country_name = country_name.replace('’', "'")  # Replace smart quotes with standard quotes
        return country_name

    # Collect the updates from the database based on the extracted countries
    def collect_updates(self, countries):
        updates = []
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    cursor.execute("SELECT [COUNTRY_NAME_FR] FROM TblSanctionsMap")
                    all_countries = cursor.fetchall()

                    for country_row in all_countries:
                        country_name = unidecode(country_row[0].strip().upper())
                        status = 'YES' if country_name in countries else 'NO'
                        updates.append((country_name, status))
        except pyodbc.Error as e:
            logging.error(f"Error collecting updates from database: {e}")
        return updates

    # Update the database with the new French tax data
    # Update the database with the new French tax data
    def update_database_FRtax(self, updates):
        yes_countries = []  # List to store the countries that have been set to 'YES'

        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    # Drop computed columns that depend on the column being updated
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

                    # Alter the column structure if necessary
                    logging.info("Altering the column for storing the values properly...")
                    cursor.execute("""
                        ALTER TABLE TblSanctionsMap
                        ALTER COLUMN [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] NVARCHAR(50)
                    """)
                    cnx.commit()

                    # Step 1: Update specified countries to 'YES' in bulk
                    if updates:
                        # Extract country names where the status is 'YES'
                        country_names_yes = [self.normalize_country_name(country_name) for country_name, status in
                                             updates
                                             if status == 'YES']

                        # Process in batches to avoid exceeding parameter limits
                        batch_size = 100  # Adjust batch size based on the parameter limits
                        logging.info(f"Updating {len(country_names_yes)} countries to 'YES'...")

                        for i in range(0, len(country_names_yes), batch_size):
                            batch = country_names_yes[i:i + batch_size]
                            placeholders = ', '.join(['?'] * len(batch))  # Create placeholders for SQL query
                            update_yes_query = f"""
                                UPDATE TblSanctionsMap
                                SET [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES'
                                WHERE REPLACE([COUNTRY_NAME_FR], '’', '''') IN ({placeholders})
                            """
                            # Execute the update query with the batch of country names
                            cursor.execute(update_yes_query, tuple(batch))
                            yes_countries.extend(batch)  # Add the updated countries to the list
                            logging.info(f"Updated {len(batch)} countries to 'YES' in the batch.")
                            cnx.commit()

                        # Step 2: Set countries that are not in the 'YES' list to 'NO'
                        logging.info("Updating countries that are NOT in the 'YES' list to 'NO'...")
                        if yes_countries:
                            placeholders = ', '.join(['?'] * len(yes_countries))
                            update_no_query = f"""
                                UPDATE TblSanctionsMap
                                SET [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO'
                                WHERE REPLACE([COUNTRY_NAME_FR], '’', '''') NOT IN ({placeholders})
                            """
                            cursor.execute(update_no_query, tuple(yes_countries))
                            logging.info("Set remaining countries to 'NO'.")
                            cnx.commit()

                    # Step 3: Recreate the dropped computed columns
                    logging.info("Recreating computed columns...")
                    cursor.execute(get_sanctions_map_columns_sql())
                    cnx.commit()
                    logging.info("Recreated computed columns successfully.")

                    # Step 4: Print out the countries that were set to 'YES'
                    if yes_countries:
                        logging.info("\nCountries updated to 'YES':")
                        for country in yes_countries:
                            logging.info(country)

        except pyodbc.Error as e:
            logging.error(f"Error updating SQL database: {e}")
        except Exception as e:
            logging.error(f"General error during FR tax updates: {e}")

    # Check for changes in the database
    def check_database_changes_FRtax(self, updates):
        changes = []
        try:
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    for country_name, new_status in updates:
                        cursor.execute(
                            "SELECT [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] FROM TblSanctionsMap WHERE [COUNTRY_NAME_FR] = ?",
                            country_name
                        )
                        result = cursor.fetchone()
                        if result:
                            old_status = result[0] if result[0] is not None else 'NO'
                            if old_status.lower() != new_status.lower():
                                changes.append((country_name, old_status, new_status))
                                if new_status.upper() == 'YES':
                                    logging.info(f"Country: {country_name}, Old Status: {old_status}, New Status: {new_status}")
        except pyodbc.Error as e:
            logging.error(f"Error checking database changes: {e}")
        return changes

def main():
    # Define the database name and the URL to fetch the data from
    db_name = 'AXIOM_PARIS'

    # URL to fetch the data from
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
