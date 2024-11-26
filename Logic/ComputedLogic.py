# sanctions_map_columns.py

def get_sanctions_map_columns_sql():
    return """
    ALTER TABLE TblSanctionsMap
    ADD 
    LEVEL_OF_RISK AS (
            CASE
                    WHEN ([FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_ENG] = 'CUBA') THEN 'PROHIBITED'
                    WHEN ([EU_AML_HIGH_RISK_COUNTRIES] = 'YES' OR 
                          [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                          [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES' OR
                          [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES') 
                         AND [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'HIGH'
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
                          [EU_FINANCIAL_MEASURES] = 'YES') 
                         AND [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' 
                         AND [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                         AND [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [UK_FINANCIAL_SANCTIONS] = 'NO' THEN 'HIGH'
                    WHEN [CPI_SCORE] >= 50 THEN 'STANDARD'
                    WHEN ([UK_FINANCIAL_SANCTIONS] = 'YES' OR
                        [US_OFAC_SANCTIONS] = 'YES') AND
                        ([FR_SECTORAL_EMBARGO] = 'NO' AND 
                          [FR_MILITARY_EMBARGO] = 'NO' AND 
                          [FR_INTERNAL_REPRESSION_EQUIPMENT] = 'NO' AND 
                          [FR_INTERNAL_REPRESSION] = 'NO' AND 
                          [FR_SECTORAL_RESTRICTIONS] = 'NO' AND 
                          [FR_FINANCIAL_RESTRICTIONS] = 'NO' AND
                          [FR_TRAVEL_BANS] = 'NO' AND 
                          [EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'NO' AND 
                          [EU_INVESTMENTS] = 'NO' AND 
                          [FR_ASSET_FREEEZE] = 'NO' AND 
                          [EU_FINANCIAL_MEASURES] = 'NO' AND
                          [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' AND
                          [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' AND
                          [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' AND
                          [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO') 
                         THEN 'STANDARD'
            ELSE 'MEDIUM'
            END
            ),
            LEVEL_OF_VIGILANCE AS (
            CASE
                    WHEN ([FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_ENG] = 'CUBA') THEN 'PROHIBITED'
                    WHEN ([EU_AML_HIGH_RISK_COUNTRIES] = 'YES' OR 
                          [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                          [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES' OR
                          [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES') 
                         AND [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'ENHANCED'
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
                          [EU_FINANCIAL_MEASURES] = 'YES') 
                         AND [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' 
                         AND [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                         AND [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [UK_FINANCIAL_SANCTIONS] = 'NO' THEN 'ENHANCED'
                    WHEN [CPI_SCORE] >= 50 THEN 'STANDARD'
                    WHEN ([UK_FINANCIAL_SANCTIONS] = 'YES' OR
                        [US_OFAC_SANCTIONS] = 'YES') AND
                        ([FR_SECTORAL_EMBARGO] = 'NO' AND 
                          [FR_MILITARY_EMBARGO] = 'NO' AND 
                          [FR_INTERNAL_REPRESSION_EQUIPMENT] = 'NO' AND 
                          [FR_INTERNAL_REPRESSION] = 'NO' AND 
                          [FR_SECTORAL_RESTRICTIONS] = 'NO' AND 
                          [FR_FINANCIAL_RESTRICTIONS] = 'NO' AND
                          [FR_TRAVEL_BANS] = 'NO' AND 
                          [EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'NO' AND 
                          [EU_INVESTMENTS] = 'NO' AND 
                          [FR_ASSET_FREEEZE] = 'NO' AND 
                          [EU_FINANCIAL_MEASURES] = 'NO' AND
                          [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' AND
                          [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' AND
                          [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' AND
                          [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO') 
                         THEN 'ENHANCED UK/US'
            ELSE 'ENHANCED'
            END
            ),
            LIST AS (
            CASE
                WHEN ([FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'YES' OR [COUNTRY_NAME_ENG] = 'CUBA') THEN 'PROHIBITED'
                    WHEN ([EU_AML_HIGH_RISK_COUNTRIES] = 'YES' OR 
                          [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'YES' OR 
                          [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES' OR
                          [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'YES') 
                         AND [FATF_HIGH_RISK_JURISDICTIONS_SUBJECT_TO_A_CALL_FOR_ACTION] = 'NO' THEN 'RED'
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
                          [EU_FINANCIAL_MEASURES] = 'YES') 
                         AND [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' 
                         AND [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' 
                         AND [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' 
                         AND [UK_FINANCIAL_SANCTIONS] = 'NO' THEN 'RED'
                    WHEN [CPI_SCORE] >= 50 THEN 'GREEN'
                    WHEN ([UK_FINANCIAL_SANCTIONS] = 'YES' OR
                        [US_OFAC_SANCTIONS] = 'YES') AND
                        ([FR_SECTORAL_EMBARGO] = 'NO' AND 
                          [FR_MILITARY_EMBARGO] = 'NO' AND 
                          [FR_INTERNAL_REPRESSION_EQUIPMENT] = 'NO' AND 
                          [FR_INTERNAL_REPRESSION] = 'NO' AND 
                          [FR_SECTORAL_RESTRICTIONS] = 'NO' AND 
                          [FR_FINANCIAL_RESTRICTIONS] = 'NO' AND
                          [FR_TRAVEL_BANS] = 'NO' AND 
                          [EU_ASSET_FREEZE_AND_PROHIBITION_TO_MAKE_FUNDS_AVAILABLE] = 'NO' AND 
                          [EU_INVESTMENTS] = 'NO' AND 
                          [FR_ASSET_FREEEZE] = 'NO' AND 
                          [EU_FINANCIAL_MEASURES] = 'NO' AND
                          [EU_AML_HIGH_RISK_COUNTRIES] = 'NO' AND
                          [FATF_JURISDICTIONS_UNDER_INCREASED_MONITORING] = 'NO' AND
                          [EU_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO' AND
                          [FR_LIST_OF_NON_COOPERATIVE_JURISDICTIONS] = 'NO') 
                         THEN 'GREEN'
            ELSE 'AMBER'
            END
    )
    """