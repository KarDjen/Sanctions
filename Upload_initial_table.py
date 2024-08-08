import pyodbc
import pandas as pd
from openpyxl import load_workbook

class ConnectToDb:
    db_name = ''
    conn_str = ''

    def __init__(self, db_name):
        ConnectToDb.db_name = db_name
        ConnectToDb.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )

    @staticmethod
    def get_data_from_sql(request):
        try:
            cnx = pyodbc.connect(ConnectToDb.conn_str)
            df = pd.read_sql(request, cnx)
            cnx.close()
            return df
        except Exception as e:
            print(f"Error reading from SQL: {e}")
            return None

    @staticmethod
    def execute_sql_command(command):
        try:
            cnx = pyodbc.connect(ConnectToDb.conn_str)
            cursor = cnx.cursor()
            cursor.execute(command)
            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            print(f"Error executing SQL command: {e}")

    @staticmethod
    def insert_data_to_sql(dataframe, table_name):
        try:
            cnx = pyodbc.connect(ConnectToDb.conn_str)
            cursor = cnx.cursor()
            for index, row in dataframe.iterrows():
                sql = f"""
                    INSERT INTO {table_name} (
                        [COUNTRY_NAME_(ENG)], [COUNTRY_NAME_(FR)], [COUNTRY_CODE_(ISO_2)], 
                        [COUNTRY_CODE_(ISO_3)], [LIST], [LEVEL_OF_RISK], [LEVEL_OF_VIGILANCE],
                        [CPI_SCORE], [CPI_RANK], [FR__-_ASSET_FREEEZE], [FR_-_SECTORAL_EMBARGO],
                        [FR_-_MILITARY__EMBARGO], [FR_-_INTERNAL_REPRESSION_EQUIPMENT],
                        [FR_-_INTERNAL_REPRESSION], [FR_-_SECTORAL_RESTRICTIONS],
                        [FR_-_FINANCIAL_RESTRICTIONS], [FR_-_TRAVEL_BANS],
                        [EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE],
                        [EU_-_INVESTMENTS], [EU_-_FINANCIAL_MEASURES],
                        [EU_-_AML_HIGH-RISK_COUNTRIES], [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)],
                        [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION],
                        [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING],
                        [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS],
                        [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS],
                        [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)]
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                cursor.execute(sql, row.values.tolist())
            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            print(f"Error inserting data into SQL: {e}")

    @staticmethod
    def drop_table_if_exists(table_name):
        try:
            cnx = pyodbc.connect(ConnectToDb.conn_str)
            cursor = cnx.cursor()
            cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            print(f"Error dropping table {table_name}: {e}")

    @staticmethod
    def update_sql_logic():
        try:
            cnx = pyodbc.connect(ConnectToDb.conn_str)
            cursor = cnx.cursor()

            # SQL logic adapted from the provided Excel formula
            sql_update = """
            UPDATE TblCountries_New
            SET 
                [LIST] = CASE
                    WHEN ([FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_(ENG)] = 'CUBA') THEN 'BLACK'
                    WHEN ([EU_-_AML_HIGH-RISK_COUNTRIES] = 'YES' OR 
                          [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                          [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'YES' OR 
                          [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR 
                          [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'YES') 
                         AND [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'GREY'
                    WHEN ([FR_-_SECTORAL_EMBARGO] = 'YES' OR 
                          [FR_-_MILITARY__EMBARGO] = 'YES' OR 
                          [FR_-_INTERNAL_REPRESSION_EQUIPMENT] = 'YES' OR 
                          [FR_-_INTERNAL_REPRESSION] = 'YES' OR 
                          [FR_-_SECTORAL_RESTRICTIONS] = 'YES' OR 
                          [FR_-_FINANCIAL_RESTRICTIONS] = 'YES' OR
                          [FR_-_TRAVEL_BANS] = 'YES' OR 
                          [EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'YES' OR 
                          [EU_-_INVESTMENTS] = 'YES' OR 
                          [FR__-_ASSET_FREEEZE] = 'YES' OR 
                          [EU_-_FINANCIAL_MEASURES] = 'YES' OR 
                          [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] = 'YES') 
                         AND [EU_-_AML_HIGH-RISK_COUNTRIES] = 'NO' 
                         AND [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                         AND [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] = 'NO' THEN 'RED'
                    WHEN [CPI_SCORE] >= 50 THEN 'GREEN'
                    ELSE 'AMBER'
                END,
                [LEVEL_OF_RISK] = CASE
                    WHEN ([FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_(ENG)] = 'CUBA') THEN 'PROHIBITED'
                    WHEN ([EU_-_AML_HIGH-RISK_COUNTRIES] = 'YES' OR 
                          [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                          [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'YES' OR 
                          [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR 
                          [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'YES') 
                         AND [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'HIGH RISK OR PROHIBITED'
                    WHEN ([FR_-_SECTORAL_EMBARGO] = 'YES' OR 
                          [FR_-_MILITARY__EMBARGO] = 'YES' OR 
                          [FR_-_INTERNAL_REPRESSION_EQUIPMENT] = 'YES' OR 
                          [FR_-_INTERNAL_REPRESSION] = 'YES' OR 
                          [FR_-_SECTORAL_RESTRICTIONS] = 'YES' OR 
                          [FR_-_FINANCIAL_RESTRICTIONS] = 'YES' OR
                          [FR_-_TRAVEL_BANS] = 'YES' OR 
                          [EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'YES' OR 
                          [EU_-_INVESTMENTS] = 'YES' OR 
                          [FR__-_ASSET_FREEEZE] = 'YES' OR 
                          [EU_-_FINANCIAL_MEASURES] = 'YES' OR 
                          [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] = 'YES') 
                         AND [EU_-_AML_HIGH-RISK_COUNTRIES] = 'NO' 
                         AND [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                         AND [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] = 'NO' THEN 'HIGH RISK'
                    WHEN [CPI_SCORE] >= 50 THEN 'LOW RISK'
                    ELSE 'MEDIUM RISK'
                END,
                [LEVEL_OF_VIGILANCE] = CASE
                    WHEN ([FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_(ENG)] = 'CUBA') THEN 'PROHIBITED'
                    WHEN ([EU_-_AML_HIGH-RISK_COUNTRIES] = 'YES' OR 
                          [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                          [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'YES' OR 
                          [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR 
                          [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'YES') 
                         AND [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'ENHANCED VIGILANCE OR PROHIBITED'
                    WHEN ([FR_-_SECTORAL_EMBARGO] = 'YES' OR 
                          [FR_-_MILITARY__EMBARGO] = 'YES' OR 
                          [FR_-_INTERNAL_REPRESSION_EQUIPMENT] = 'YES' OR 
                          [FR_-_INTERNAL_REPRESSION] = 'YES' OR 
                          [FR_-_SECTORAL_RESTRICTIONS] = 'YES' OR 
                          [FR_-_FINANCIAL_RESTRICTIONS] = 'YES' OR
                          [FR_-_TRAVEL_BANS] = 'YES' OR 
                          [EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'YES' OR 
                          [EU_-_INVESTMENTS] = 'YES' OR 
                          [FR__-_ASSET_FREEEZE] = 'YES' OR 
                          [EU_-_FINANCIAL_MEASURES] = 'YES' OR 
                          [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] = 'YES') 
                         AND [EU_-_AML_HIGH-RISK_COUNTRIES] = 'NO' 
                         AND [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                         AND [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] = 'NO' THEN 'ENHANCED VIGILANCE'
                    WHEN [CPI_SCORE] >= 50 THEN 'STANDARD VIGILANCE'
                    ELSE 'ENHANCED VIGILANCE'
                END
            """

            cursor.execute(sql_update)
            cnx.commit()
            cursor.close()
            cnx.close()
        except Exception as e:
            print(f"Error executing SQL logic update: {e}")

def read_spreadsheet(spreadsheet_path):
    # Load the workbook
    workbook = load_workbook(spreadsheet_path)
    sheet = workbook.active

    # Convert sheet to DataFrame
    data = sheet.values
    columns = next(data)[0:]  # Assuming the first row is the header
    columns = [col.replace('\n', ' ').replace(' ', '_').upper() for col in columns]  # Normalize column names
    df = pd.DataFrame(data, columns=columns)

    print("Columns in the DataFrame:", df.columns)  # Add this line to print the column names

    return df, workbook, sheet

def print_table_content_as_dataframe(table_name):
    try:
        cnx = pyodbc.connect(ConnectToDb.conn_str)
        df = pd.read_sql(f"SELECT * FROM {table_name}", cnx)
        pd.set_option('display.max_columns', None)  # Ensure all columns are printed
        cnx.close()
        return df
    except Exception as e:
        print(f"Error reading table {table_name}: {e}")
        return None

def list_all_tables():
    try:
        cnx = pyodbc.connect(ConnectToDb.conn_str)
        cursor = cnx.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        tables = cursor.fetchall()
        print("All tables in the database:")
        for table in tables:
            print(table[0])
        cursor.close()
        cnx.close()
    except Exception as e:
        print(f"Error listing all tables: {e}")

def update_spreadsheet(df, workbook, sheet, updated_data):
    # Update the dataframe with the updated data
    for index, row in updated_data.iterrows():
        for col_num, value in enumerate(row, start=1):
            sheet.cell(row=index + 2, column=col_num, value=value)

    # Save the updated workbook
    workbook.save('updated_' + spreadsheet_path)

def main():
    spreadsheet_path = 'U:\\New folder1\\BookTest.xlsx'
    new_table_name = 'TblCountries_New'
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'

    db = ConnectToDb(db_name)

    print("Reading data from the spreadsheet...")
    df, workbook, sheet = read_spreadsheet(spreadsheet_path)

    # Convert appropriate columns to numeric
    df['CPI_SCORE'] = pd.to_numeric(df['CPI_SCORE'], errors='coerce')
    df['CPI_RANK'] = pd.to_numeric(df['CPI_RANK'], errors='coerce')

    # Handle NaN values - here, we fill NaN with 0; adjust as needed
    df['CPI_SCORE'] = df['CPI_SCORE'].fillna(0)
    df['CPI_RANK'] = df['CPI_RANK'].fillna(0)

    # Check maximum length of data in each column
    column_lengths = {col: max(df[col].astype(str).apply(len)) for col in df.columns}
    print("Column lengths:", column_lengths)

    print("Dropping the existing table if it exists...")
    ConnectToDb.drop_table_if_exists(new_table_name)
    print(f"Table {new_table_name} dropped successfully if it existed.")

    print("Creating new table...")
    create_table_command = f"""
        CREATE TABLE {new_table_name} (
            [COUNTRY_NAME_(ENG)] NVARCHAR({column_lengths['COUNTRY_NAME_(ENG)']}),
            [COUNTRY_NAME_(FR)] NVARCHAR({column_lengths['COUNTRY_NAME_(FR)']}),
            [COUNTRY_CODE_(ISO_2)] NVARCHAR({column_lengths['COUNTRY_CODE_(ISO_2)']}),
            [COUNTRY_CODE_(ISO_3)] NVARCHAR({column_lengths['COUNTRY_CODE_(ISO_3)']}),
            [LIST] NVARCHAR({column_lengths['LIST']}),
            [LEVEL_OF_RISK] NVARCHAR({column_lengths['LEVEL_OF_RISK']}),
            [LEVEL_OF_VIGILANCE] NVARCHAR({column_lengths['LEVEL_OF_VIGILANCE']}),
            [CPI_SCORE] INT,
            [CPI_RANK] INT,
            [FR__-_ASSET_FREEEZE] NVARCHAR({column_lengths['FR__-_ASSET_FREEEZE']}),
            [FR_-_SECTORAL_EMBARGO] NVARCHAR({column_lengths['FR_-_SECTORAL_EMBARGO']}),
            [FR_-_MILITARY__EMBARGO] NVARCHAR({column_lengths['FR_-_MILITARY__EMBARGO']}),
            [FR_-_INTERNAL_REPRESSION_EQUIPMENT] NVARCHAR({column_lengths['FR_-_INTERNAL_REPRESSION_EQUIPMENT']}),
            [FR_-_INTERNAL_REPRESSION] NVARCHAR({column_lengths['FR_-_INTERNAL_REPRESSION']}),
            [FR_-_SECTORAL_RESTRICTIONS] NVARCHAR({column_lengths['FR_-_SECTORAL_RESTRICTIONS']}),
            [FR_-_FINANCIAL_RESTRICTIONS] NVARCHAR({column_lengths['FR_-_FINANCIAL_RESTRICTIONS']}),
            [FR_-_TRAVEL_BANS] NVARCHAR({column_lengths['FR_-_TRAVEL_BANS']}),
            [EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] NVARCHAR({column_lengths['EU_-_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE']}),
            [EU_-_INVESTMENTS] NVARCHAR({column_lengths['EU_-_INVESTMENTS']}),
            [EU_-_FINANCIAL_MEASURES] NVARCHAR({column_lengths['EU_-_FINANCIAL_MEASURES']}),
            [EU_-_AML_HIGH-RISK_COUNTRIES] NVARCHAR({column_lengths['EU_-_AML_HIGH-RISK_COUNTRIES']}),
            [US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)] NVARCHAR({column_lengths['US_-_OFAC__SANCTION_PROGRAM__(PER_COUNTRY)']}),
            [FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] NVARCHAR({column_lengths['FATF_-_HIGH-RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION']}),
            [FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING] NVARCHAR({column_lengths['FATF_-_JURISDICTIONS_UNDER_INCREASED_MONITORING']}),
            [EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS] NVARCHAR({column_lengths['EU_-_LIST_OF_NON-COOPERATIVE_JURISDICTIONS']}),
            [FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS] NVARCHAR({column_lengths['FR__-__LIST_OF_NON-COOPERATIVE_JURISDICTIONS']}),
            [UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)] NVARCHAR({column_lengths['UK_FINANCIAL_SANCTIONS_PROGRAM_(PER_PROGRAM)']})
        )
    """
    ConnectToDb.execute_sql_command(create_table_command)
    print("New table created successfully.")

    print("Inserting data into the new table...")
    ConnectToDb.insert_data_to_sql(df, new_table_name)
    print("Data inserted successfully.")

    print("Applying SQL logic with additional logic...")
    ConnectToDb.update_sql_logic()
    print("SQL logic applied successfully.")

    print("Retrieving and displaying updated table content:")
    updated_df = print_table_content_as_dataframe(new_table_name)
    print(updated_df)

    # Export the updated DataFrame to Excel in the root folder
    output_path = 'updated_table.xlsx'
    updated_df.to_excel(output_path, index=False)
    print(f"Updated table exported to {output_path}")

    # Assuming some updates are done to the DataFrame (df)
    # Here you would include any logic to update the DataFrame as needed
    # For example, let's just update a column value for demonstration purposes
    df['CPI_SCORE'] = df['CPI_SCORE'].apply(lambda x: x + 1 if pd.notnull(x) else x)

    # Update the spreadsheet with new data
    print("Updating the spreadsheet with new data...")
    update_spreadsheet(df, workbook, sheet, df)
    print("Spreadsheet updated successfully.")

if __name__ == "__main__":
    main()
