# Sanctions Pipeline Automation 🚀

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.7%2B-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)

## Table of Contents
- [Overview](#overview)
- [Features](#features)
    - [1. Data Fetching and Parsing](#1-data-fetching-and-parsing)
    - [2. Change Auditing](#2-change-auditing)
    - [3. Data Export](#3-data-export)
    - [4. Modular Design](#4-modular-design)
- [File Structure](#file-structure)
- [Prerequisites](#prerequisites)
    - [Software](#software)
    - [Environment Setup](#environment-setup)
- [Database Schema](#database-schema)
- [Usage](#usage)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [Extending the Project](#extending-the-project)
    - [Add a New Parser](#add-a-new-parser)
    - [Modify Export Logic](#modify-export-logic)
- [Example Output](#example-output)
- [Support](#support)
- [License](#license)

## Overview

**Sanctions Pipeline Automation** automates the process of updating, auditing, and exporting a comprehensive list of countries subject to sanctions across multiple jurisdictions:

- **Jurisdictions Covered:**
    - France
    - European Union (EU)
    - United Kingdom (UK)
    - United States (US)

- **Additional Lists:**
    - EU list of non-cooperative jurisdictions for tax purposes
    - French (FR) list of non-cooperative jurisdictions for tax purposes
    - FATF lists:
        - High-risk jurisdictions subject to a call for action
        - Jurisdictions under increased monitoring
    - Corruption Perceptions Index

This project integrates various parser modules, updates a centralized database, logs changes, and exports the results to Excel files for further analysis and auditing. It is designed to be flexible and scalable, seamlessly integrating into existing systems.

## Features

### 1. Data Fetching and Parsing

Multiple parser modules are provided to update sanctions data from various official sources:

1. **France:** [Sanctions Économiques](https://www.tresor.economie.gouv.fr/services-aux-entreprises/sanctions-economiques/{url_country_name})
2. **EU:** [EU Sanctions List](https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:32020D1998&from=EN)
3. **UK:** [UK Financial Sanctions](https://www.gov.uk/government/collections/financial-sanctions-regime-specific-consolidated-lists-and-releases)
4. **US:** [OFAC SDN List](https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV)
5. **EU Non-Cooperative Jurisdictions (Tax):** [EU List](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A52024XG01804)
6. **FR Non-Cooperative Jurisdictions (Tax):** [French List](https://www.douane.gouv.fr/actualites/lcb-ft-liste-des-etats-et-territoires-non-cooperatifs-en-matiere-fiscale)
7. **FATF High-Risk Jurisdictions:** [FATF Call for Action](https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/Call-for-action-{}-{}.html)
8. **FATF Increased Monitoring:** [FATF Monitoring](https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/increased-monitoring-{}-{}.html)
9. **Corruption Perceptions Index:** [Transparency International](https://www.transparency.org/en/countries/{formatted_country_name})

### 2. Change Auditing

Compares old and new data to log updates in the `TblSanctionsMap_Audit` table, ensuring transparency and traceability of changes.

### 3. Data Export

Exports updated data and audit logs to Excel files for comprehensive reporting and analysis.

### 4. Modular Design

A flexible structure allows for easy addition of new parsers or modifications, ensuring scalability and adaptability to evolving requirements.

## File Structure

## File Structure

- `main.py`  
  *Main entry point for the pipeline*
- `Logic/`  
  *Directory containing all business logic*
  - `ComputedLogic.py`  
    *Logic for computing changes*
- `Parser/`  
  *Directory containing all parser modules*
  - `CPI.py`  
    *Corruption Perceptions Index parser*
  - `EUFATF.py`  
    *EU FATF parser*
  - `EUsanctions.py`  
    *EU Sanctions List parser*
  - `EUtax.py`  
    *EU Tax non-cooperative jurisdictions parser*
  - `FATF_CfA.py`  
    *FATF Call for Action parser*
  - `FATF_IM.py`  
    *FATF Increased Monitoring parser*
  - `FRsanctions.py`  
    *French Sanctions parser*
  - `FRtax.py`  
    *French Tax non-cooperative jurisdictions parser*
  - `OFAC.py`  
    *OFAC SDN List parser*
  - `UKsanctions.py`  
    *UK Sanctions List parser*
- `env/`  
  *Environment variables configuration*
- `requirements.txt`  
  *Python dependencies*
- `README.md`  
  *Project documentation*



## Prerequisites

### Software

- **Python:** 3.7+
- **Libraries:**
    - `os`
    - `datetime`
    - `logging`
    - `pyodbc`
    - `dotenv`
    - `openpyxl`
- **Database:**
    - SQL Server

### Environment Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/KarDjen/sanctions-pipeline-automation.git
   cd sanctions-pipeline-automation

2. **Create a Virtual Environment (Optional but Recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
   
3. **Install Dependencies:**
    ```bash
    pip install -r requirements.txt

4. **Create a `.env` file in the root directory with the following variables:**
    ```bash
    SERVER=your_server_name
    DATABASE=your_database_name
    UID=your_database_username
    PWD=your_database_password
    EXPORT_FOLDER=optional_export_directory


## Database Schema

### Tables

1. **TblSanctionsMap:** Stores the latest sanctions data for each jurisdiction.

   The SQL script to create this table is as follows:

   ```sql
   CREATE TABLE TblSanctionsMap (
       SanctionsMapId INT IDENTITY(1,1) PRIMARY KEY,
       COUNTRY_NAME_ENG NVARCHAR(255),
       COUNTRY_NAME_FR NVARCHAR(255),
       COUNTRY_CODE_ISO_2 NVARCHAR(2),
       COUNTRY_CODE_ISO_3 NVARCHAR(3),
       CPI_SCORE INT,
       CPI_RANK INT,
       FR_ASSET_FREEZE NVARCHAR(255), 
       FR_SECTORAL_EMBARGO NVARCHAR(255),
       FR_MILITARY_EMBARGO NVARCHAR(255),
       FR_INTERNAL_REPRESSION_EQUIPMENT NVARCHAR(255),
       FR_INTERNAL_REPRESSION NVARCHAR(255),
       FR_SECTORAL_RESTRICTIONS NVARCHAR(255),
       FR_FINANCIAL_RESTRICTIONS NVARCHAR(255),
       FR_TRAVEL_BANS NVARCHAR(255),
       EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE NVARCHAR(255),
       EU_INVESTMENTS NVARCHAR(255),
       EU_FINANCIAL_MEASURES NVARCHAR(255),
       EU_AML_HIGH_RISK_COUNTRIES NVARCHAR(255),
       US_OFAC_SANCTIONS NVARCHAR(255),
       FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION NVARCHAR(255),
       FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING NVARCHAR(255),
       EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS NVARCHAR(255),
       FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS NVARCHAR(255),
       UK_FINANCIAL_SANCTIONS NVARCHAR(255),
       LEVEL_OF_RISK NVARCHAR(255),
       LEVEL_OF_VIGILANCE NVARCHAR(255),
       LIST NVARCHAR(255)
   );

2. **TblSanctionsMap_Audit:** Logs changes to the `TblSanctionsMap` table.

    The SQL script to create this table is as follows:

    ```sql 
    CREATE TABLE TblSanctionsMap_Audit (
        AuditID INT IDENTITY(1,1) PRIMARY KEY,
        SanctionsMapId INT,
        ColumnName NVARCHAR(255),
        OldValue NVARCHAR(MAX),
        NewValue NVARCHAR(MAX),
        UpdatedAt DATETIME
    );

### Usage

1. **Run the Pipeline:**
    Ensure the database connection and parsers are properly configured.

    ```bash
    python main.py
   
2. **Check Exported Files:**
3. **Navigate to the `EXPORT_FOLDER` (default is the project root) to find the exported Excel files:**

    - `Sanctions_Matrix_YYYY-MM-DD_HH-MM-SS.xlsx`
    - `TblSanctionsMap_Audit_Export_YYYY-MM-DD_HH-MM-SS.xlsx`

### Logging

Logs are output to the console in real-time and include:

- **Change Logs:** Details of detected and logged changes.
- **Errors:** Information about errors encountered during parsing or database operations.
- **Export Paths:** Locations of the exported Excel files.

### Error Handling

- **Database Rollback:** Transactions are automatically rolled back in case of errors to maintain data integrity.
- **Error Logging:** All errors are logged with stack traces to facilitate debugging.

### Extending the Project

#### Add a New Parser

1. **Create a New Module:**
    - Add a new Python module in the `Parser` directory with a `main()` function that handles the specific data fetching and parsing logic.
    - Register the Parser: Update the `updaters` list in `main.py` to include the new parser module.

#### Modify Export Logic

1. **Update Export Functions:**
    - Modify the `export_database_to_excel` or `export_table_to_excel` functions in `main.py` as needed to accommodate changes in the data structure or export requirements.

### Support

For questions or support, please: 

- Open an issue in the GitHub repository
- Consult the console logs for troubleshooting.

### License

This project is licensed under the MIT License.