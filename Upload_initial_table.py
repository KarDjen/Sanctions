"""
This script is responsible for uploading the initial table to the database. It reads data from a spreadsheet,
creates a new table in the database, and inserts the data into the new table. It also logs changes to an audit table,
which keeps track of the changes made to the data over time for auditing purposes. The audit table is built one line one
for each change made to the data.

The script uses the pyodbc library to connect to the SQL Server database and pandas to read data from the spreadsheet.
The script also uses the openpyxl library to load the spreadsheet and extract data from it.
"""

# Import necessary libraries
import pyodbc
import pandas as pd
from openpyxl import load_workbook
import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define a class to connect to the databaset
class ConnectToDb:

    # Initialize the class with the database name and connection string (to locate in .env file)
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )

    # Method to execute SQL commands
    def execute_sql_command(self, command):
        # Try to connect to the database and execute the command
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute(command)
            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.error(f"Error executing SQL command: {e}")

    # Method to insert data into the database
    def insert_data_to_sql(self, dataframe, table_name):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()

            # Define insertable columns (Non-computed columns without square brackets)
            insert_columns = [
                "COUNTRY_NAME_ENG", "COUNTRY_NAME_FR", "COUNTRY_CODE_ISO_2", "COUNTRY_CODE_ISO_3",
                "CPI_SCORE", "CPI_RANK", "FR_ASSET_FREEEZE", "FR_SECTORAL_EMBARGO",
                "FR_MILITARY_EMBARGO", "FR_INTERNAL_REPRESSION_EQUIPMENT", "FR_INTERNAL_REPRESSION",
                "FR_SECTORAL_RESTRICTIONS", "FR_FINANCIAL_RESTRICTIONS", "FR_TRAVEL_BANS",
                "EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE", "EU_INVESTMENTS",
                "EU_FINANCIAL_MEASURES", "EU_AML_HIGH_RISK_COUNTRIES", "US_OFAC_SANCTIONS",
                "FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION",
                "FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING",
                "EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS", "FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS",
                "UK_FINANCIAL_SANCTIONS"
            ]

            # Define the SQL column names (with square brackets for SQL syntax)
            sql_insert_columns = [f"[{col}]" for col in insert_columns]

            placeholders = ",".join(["?"] * len(insert_columns))

            # Generate SQL query
            sql_template = f"""
                INSERT INTO {table_name} (
                    {", ".join(sql_insert_columns)}
                ) VALUES ({placeholders})
            """

            for index, row in dataframe.iterrows():
                values_to_insert = row[insert_columns].tolist()

                if len(values_to_insert) == len(insert_columns):
                    # Log the query and the values for debugging purposes
                    logging.info(f"Executing SQL: {sql_template}")
                    logging.info(f"With Values: {values_to_insert}")

                    cursor.execute(sql_template, values_to_insert)
                else:
                    logging.warning(f"Skipping row {index} due to mismatched column count.")

            cnx.commit()
            cursor.close()
            cnx.close()

            logging.info("Data inserted successfully.")

        except Exception as e:
            logging.error(f"Error inserting data into SQL: {e}")

    # Method to drop a table if it exists for resetting purpose
    def drop_table_if_exists(self, table_name):
        try:
            cnx = pyodbc.connect(self.conn_str)
            cursor = cnx.cursor()
            cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
            cnx.commit()
            cursor.close()
            cnx.close()
            logging.info(f"Table {table_name} dropped successfully.")
        except Exception as e:
            logging.error(f"Error dropping table {table_name}: {e}")

    # Method to create an audit table for logging changes to TblSanctionsMap
    def create_audit_table(self):
        try:
            # Connect to the database
            with pyodbc.connect(self.conn_str) as cnx:
                with cnx.cursor() as cursor:
                    # Drop the TblSanctionsMap_Audit table if it exists
                    cursor.execute("""
                        IF OBJECT_ID('TblSanctionsMap_Audit', 'U') IS NOT NULL
                        DROP TABLE TblSanctionsMap_Audit;
                    """)
                    cnx.commit()
                    logging.info("TblSanctionsMap_Audit table dropped successfully.")

                    # Create a new TblSanctionsMap_Audit table
                    cursor.execute("""
                        CREATE TABLE TblSanctionsMap_Audit (
                            AuditID INT IDENTITY(1,1) PRIMARY KEY,
                            CountryName NVARCHAR(255),
                            ColumnName NVARCHAR(255),
                            OldValue NVARCHAR(MAX),
                            NewValue NVARCHAR(MAX),
                            UpdatedAt DATETIME DEFAULT GETDATE()
                        )
                    """)
                    cnx.commit()
                    logging.info("TblSanctionsMap_Audit table created successfully.")
        except Exception as e:
            logging.error(f"Error creating TblSanctionsMap_Audit table: {e}")

# Method to read data from the spreadsheet
def read_spreadsheet(spreadsheet_path):
    try:
        workbook = load_workbook(spreadsheet_path, data_only=True)
        sheet = workbook.active
        data = sheet.values
        columns = next(data)[0:]
        columns = [col.replace('\n', ' ').replace(' ', '_').upper() for col in columns]
        df = pd.DataFrame(data, columns=columns)

        df.rename(columns={'COUNTRY_NAME_(ENG)': 'COUNTRY_NAME_ENG'}, inplace=True)
        df.rename(columns={'COUNTRY_NAME_(FR)': 'COUNTRY_NAME_FR'}, inplace=True)
        df.rename(columns={'COUNTRY_CODE_(ISO_2)': 'COUNTRY_CODE_ISO_2'}, inplace=True)
        df.rename(columns={'COUNTRY_CODE_(ISO_3)': 'COUNTRY_CODE_ISO_3'}, inplace=True)
        df.rename(columns={'FR__-_ASSET_FREEEZE': 'FR_ASSET_FREEEZE'}, inplace=True)
        df.rename(columns={'FR_-_SECTORAL_EMBARGO': 'FR_SECTORAL_EMBARGO'}, inplace=True)
        df.rename(columns={'FR_-_MILITARY__EMBARGO': 'FR_MILITARY_EMBARGO'}, inplace=True)
        df.rename(columns={'FR_-_INTERNAL_REPRESSION_EQUIPMENT': 'FR_INTERNAL_REPRESSION_EQUIPMENT'}, inplace=True)
        df.rename(columns={'FR_-_INTERNAL_REPRESSION': 'FR_INTERNAL_REPRESSION'}, inplace=True)
        df.rename(columns={'FR_-_SECTORAL_RESTRICTIONS': 'FR_SECTORAL_RESTRICTIONS'}, inplace=True)
        df.rename(columns={'FR_-_FINANCIAL_RESTRICTIONS': 'FR_FINANCIAL_RESTRICTIONS'}, inplace=True)
        df.rename(columns={'FR_-_TRAVEL_BANS': 'FR_TRAVEL_BANS'}, inplace=True)
        df.rename(columns={'EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE': 'EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE'}, inplace=True)
        df.rename(columns={'EU_-_INVESTMENTS': 'EU_INVESTMENTS'}, inplace=True)
        df.rename(columns={'EU_-_FINANCIAL_MEASURES': 'EU_FINANCIAL_MEASURES'}, inplace=True)
        df.rename(columns={'EU_-_AML_HIGH-RISK_COUNTRIES': 'EU_AML_HIGH_RISK_COUNTRIES'}, inplace=True)
        df.rename(columns={'US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)': 'US_OFAC_SANCTIONS'}, inplace=True)
        df.rename(columns={'FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION': 'FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION'}, inplace=True)
        df.rename(columns={'FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING': 'FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING'}, inplace=True)
        df.rename(columns={'EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS': 'EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS'}, inplace=True)
        df.rename(columns={'FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS': 'FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS'}, inplace=True)
        df.rename(columns={'UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)': 'UK_FINANCIAL_SANCTIONS'}, inplace=True)



        logging.info("Spreadsheet read successfully.")
        return df, workbook, sheet
    except Exception as e:
        logging.error(f"Error reading spreadsheet: {e}")
        return None, None, None

def main():

    # Define the path to the spreadsheet, the new table name, and the database name
    spreadsheet_path = 'U:\\New folder1\\BookTest.xlsx'
    new_table_name = 'TblSanctionsMap'
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'

    # Connect to the database
    db = ConnectToDb(db_name)

    # Read data from the spreadsheet
    logging.info("Reading data from the spreadsheet...")
    df, workbook, sheet = read_spreadsheet(spreadsheet_path)

    # If the DataFrame is not None, proceed with database operations
    if df is not None:

        # Clean the column names
        df['CPI_SCORE'] = pd.to_numeric(df['CPI_SCORE'], errors='coerce').fillna(0)
        df['CPI_RANK'] = pd.to_numeric(df['CPI_RANK'], errors='coerce').fillna(0)

        # Get the maximum length of each column
        column_lengths = {col: max(df[col].astype(str).apply(len)) for col in df.columns}
        logging.info(f"Column lengths: {column_lengths}")

        # Drop the existing table if it exists
        logging.info("Dropping the existing table if it exists...")
        db.drop_table_if_exists(new_table_name)

        # Create a new table in the database
        logging.info("Creating new table...")
        create_table_command = f"""
            CREATE TABLE {new_table_name} (
                [COUNTRY_NAME_ENG] NVARCHAR({column_lengths['COUNTRY_NAME_ENG']}),
                [COUNTRY_NAME_FR] NVARCHAR({column_lengths['COUNTRY_NAME_FR']}),
                [COUNTRY_CODE_ISO_2] NVARCHAR({column_lengths['COUNTRY_CODE_ISO_2']}),
                [COUNTRY_CODE_ISO_3] NVARCHAR({column_lengths['COUNTRY_CODE_ISO_3']}),
                [CPI_SCORE] INT,
                [CPI_RANK] INT,
                [FR_ASSET_FREEEZE] NVARCHAR({column_lengths['FR_ASSET_FREEEZE']}),
                [FR_SECTORAL_EMBARGO] NVARCHAR({column_lengths['FR_SECTORAL_EMBARGO']}),
                [FR_MILITARY_EMBARGO] NVARCHAR({column_lengths['FR_MILITARY_EMBARGO']}),
                [FR_INTERNAL_REPRESSION_EQUIPMENT] NVARCHAR({column_lengths['FR_INTERNAL_REPRESSION_EQUIPMENT']}),
                [FR_INTERNAL_REPRESSION] NVARCHAR({column_lengths['FR_INTERNAL_REPRESSION']}),
                [FR_SECTORAL_RESTRICTIONS] NVARCHAR({column_lengths['FR_SECTORAL_RESTRICTIONS']}),
                [FR_FINANCIAL_RESTRICTIONS] NVARCHAR({column_lengths['FR_FINANCIAL_RESTRICTIONS']}),
                [FR_TRAVEL_BANS] NVARCHAR({column_lengths['FR_TRAVEL_BANS']}),
                [EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] NVARCHAR({column_lengths['EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE']}),
                [EU_INVESTMENTS] NVARCHAR({column_lengths['EU_INVESTMENTS']}),
                [EU_FINANCIAL_MEASURES] NVARCHAR({column_lengths['EU_FINANCIAL_MEASURES']}),
                [EU_AML_HIGH_RISK_COUNTRIES] NVARCHAR({column_lengths['EU_AML_HIGH_RISK_COUNTRIES']}),
                [US_OFAC_SANCTIONS] NVARCHAR({column_lengths['US_OFAC_SANCTIONS']}),
                [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] NVARCHAR({column_lengths['FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION']}),
                [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] NVARCHAR({column_lengths['FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING']}),
                [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] NVARCHAR({column_lengths['EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS']}),
                [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] NVARCHAR({column_lengths['FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS']}),
                [UK_FINANCIAL_SANCTIONS] NVARCHAR({column_lengths['UK_FINANCIAL_SANCTIONS']}),
                [LEVEL_OF_RISK] AS (
                    CASE
                            WHEN ([FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_ENG] = 'CUBA') THEN 'PROHIBITED'
                            WHEN ([EU_AML_HIGH_RISK_COUNTRIES] = 'YES' OR 
                                  [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                                  [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES' OR 
                                  [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR 
                                  [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES') 
                                 AND [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'HIGH RISK OR PROHIBITED'
                            WHEN ([FR_SECTORAL_EMBARGO] = 'YES' OR 
                                  [FR_MILITARY_EMBARGO] = 'YES' OR 
                                  [FR_INTERNAL_REPRESSION_EQUIPMENT] = 'YES' OR 
                                  [FR_INTERNAL_REPRESSION] = 'YES' OR 
                                  [FR_SECTORAL_RESTRICTIONS] = 'YES' OR 
                                  [FR_FINANCIAL_RESTRICTIONS] = 'YES' OR
                                  [FR_TRAVEL_BANS] = 'YES' OR 
                                  [EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'YES' OR 
                                  [EU_INVESTMENTS] = 'YES' OR 
                                  [FR_ASSET_FREEEZE] = 'YES' OR 
                                  [EU_FINANCIAL_MEASURES] = 'YES' OR 
                                  [US_OFAC_SANCTIONS] = 'YES') 
                                 AND [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' 
                                 AND [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                                 AND [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                                 AND [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                                 AND [UK_FINANCIAL_SANCTIONS] = 'NO' THEN 'HIGH RISK'
                            WHEN [CPI_SCORE] >= 50 THEN 'LOW RISK'
                    ELSE 'MEDIUM RISK'
                    END
                ),
                [LEVEL_OF_VIGILANCE] AS (
                    CASE
                            WHEN ([FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_ENG] = 'CUBA') THEN 'PROHIBITED'
                            WHEN ([EU_AML_HIGH_RISK_COUNTRIES] = 'YES' OR 
                                  [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                                  [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES' OR 
                                  [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR 
                                  [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES') 
                                 AND [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'ENHANCED VIGILANCE OR PROHIBITED'
                            WHEN ([FR_SECTORAL_EMBARGO] = 'YES' OR 
                                  [FR_MILITARY_EMBARGO] = 'YES' OR 
                                  [FR_INTERNAL_REPRESSION_EQUIPMENT] = 'YES' OR 
                                  [FR_INTERNAL_REPRESSION] = 'YES' OR 
                                  [FR_SECTORAL_RESTRICTIONS] = 'YES' OR 
                                  [FR_FINANCIAL_RESTRICTIONS] = 'YES' OR
                                  [FR_TRAVEL_BANS] = 'YES' OR 
                                  [EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'YES' OR 
                                  [EU_INVESTMENTS] = 'YES' OR 
                                  [FR_ASSET_FREEEZE] = 'YES' OR 
                                  [EU_FINANCIAL_MEASURES] = 'YES' OR 
                                  [US_OFAC_SANCTIONS] = 'YES') 
                                 AND [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' 
                                 AND [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                                 AND [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                                 AND [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                                 AND [UK_FINANCIAL_SANCTIONS] = 'NO' THEN 'ENHANCED VIGILANCE'
                            WHEN [CPI_SCORE] >= 50 THEN 'STANDARD VIGILANCE'
                            ELSE 'ENHANCED VIGILANCE'
                    END
                ),
                [LIST] AS (
                    CASE
                        WHEN ([FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_ENG] = 'CUBA') THEN 'BLACK'
                        WHEN ([EU_AML_HIGH_RISK_COUNTRIES] = 'YES' OR 
                              [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                              [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES' OR 
                              [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR 
                              [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES') 
                             AND [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'GREY'
                        WHEN ([FR_SECTORAL_EMBARGO] = 'YES' OR 
                              [FR_MILITARY_EMBARGO] = 'YES' OR 
                              [FR_INTERNAL_REPRESSION_EQUIPMENT] = 'YES' OR 
                              [FR_INTERNAL_REPRESSION] = 'YES' OR 
                              [FR_SECTORAL_RESTRICTIONS] = 'YES' OR 
                              [FR_FINANCIAL_RESTRICTIONS] = 'YES' OR
                              [FR_TRAVEL_BANS] = 'YES' OR 
                              [EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'YES' OR 
                              [EU_INVESTMENTS] = 'YES' OR 
                              [FR_ASSET_FREEEZE] = 'YES' OR 
                              [EU_FINANCIAL_MEASURES] = 'YES' OR 
                              [US_OFAC_SANCTIONS] = 'YES') 
                             AND [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' 
                             AND [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                             AND [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                             AND [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                             AND [UK_FINANCIAL_SANCTIONS] = 'NO' THEN 'RED'
                        WHEN [CPI_SCORE] >= 50 THEN 'GREEN'
                        ELSE 'AMBER'
                    END
                ),
            )
        """
        db.execute_sql_command(create_table_command)

        # Insert data into the new table
        logging.info("Inserting data into the new table...")
        db.insert_data_to_sql(df, new_table_name)

        # Create an audit table for logging changes
        logging.info("Creating audit table if it doesn't exist...")
        db.create_audit_table()

    else:
        logging.error("DataFrame is None. Unable to proceed with database operations.")

if __name__ == "__main__":
    main()
