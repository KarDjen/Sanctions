import pyodbc
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_exact_country_name(db_name):
    try:
        # Connection string setup
        conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER=SRV-SQL01\\SQL02;'
            f'DATABASE={db_name};'
            f'UID=sa;'
            f'PWD=Ax10mPar1$'
        )

        # Connect to the database
        cnx = pyodbc.connect(conn_str)
        cursor = cnx.cursor()

        # Exact match query
        query = """
                SELECT [COUNTRY_NAME_ENG] 
                FROM TblSanctionsMap 
                WHERE REPLACE([COUNTRY_NAME_ENG], '’', '''') = 'DEMOCRATIC PEOPLE''S REPUBLIC OF KOREA (DPRK - NORTH KOREA)'

                """
        cursor.execute(query)

        # Fetch and print the result
        rows = cursor.fetchall()
        if rows:
            logging.info("Countries found with the exact name:")
            for row in rows:
                print(row[0])
        else:
            logging.info("No countries found with the exact name.")

        # Close the connection
        cursor.close()
        cnx.close()

    except pyodbc.Error as e:
        logging.error(f"Error executing query: {e}")


if __name__ == "__main__":
    db_name = 'AXIOM_PARIS_TEST_CYRILLE'  # Replace with your actual database name
    fetch_exact_country_name(db_name)
