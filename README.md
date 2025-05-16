# Extract-dates-from-csv
Extracts many varying versions and formats of dates from a csv.  Assumes the rightmost column is to be searched. 

# -------------------------------------------------------------------------------  
# Contract Filename Date Extraction and Standardization Script  
#  
# SUMMARY:  
# This script is designed to extract a date "token" from typical contract or finance  
# document filenames where the date is frequently encoded as a group like "MM-DD",  
# "MM-YY", "YYYY-MM", "MM-DD-YY", etc. The script robustly parses various date-like  
# patterns, normalizes them into a 3-part (pseudo-)date string (for further parsing),  
# and then uses several date parsing libraries to standardize and cross-validate the  
# extracted date.  
#  
# EXPECTED USE CASE:  
# - You have a pandas DataFrame with a column containing filenames (or similar strings).  
# - These strings often encode a date as the rightmost date-like group (e.g.,  
#   "Contract_Smith-04-21.pdf" for April 2021, "Analysis_05_14.XLSX" for May 2014).  
# - The goal is to extract and standardize the date for downstream data integration,  
#   searching, or reporting, and to log the specific evaluated substring used for date  
#   extraction per record.  
#  
# EXTRACTION LOGIC:  
# 1. The script scans each string and attempts to find the RIGHTMOST group containing  
#    3 consecutive number segments separated by a consistent delimiter (e.g.,  
#    "12-01-08" or "2012_01_08").  
#    - Delimiters supported: "-", "_", "."  
# 2. If a 3-part group is found (e.g., "12-01-08" or "2012-12-01"), that group is used  
#    as the candidate for date parsing.  
# 3. If ONLY a 2-part group is found (e.g., "12-08", "05_14"), it is expanded by  
#    inserting "01" as the middle group, resulting in "12-01-08" or "05_01_14".  
#    - This heuristic assumes dates represent the first of the month if only month+year.  
# 4. The extracted substring (the "evaluated string element") is passed as input to  
#    multiple date parsing engines:  
#       - datefinder  
#       - dateparser  
#       - parsedatetime  
#       - regex-based parsing  
# 5. Results from the various date parsing methods are compared, and a consensus date  
#    is chosen for robustness.  
# 6. Only the rightmost date-like group in the string is captured and used; any earlier  
#    numeric groups are ignored.  
#  
# OUTPUT:  
# For each input record, the script produces:  
#   - "Evaluated string element": the exact substring that was extracted and presented to date parsers.  
#   - Four standardized date fields (from different parsers):  
#        * "datefinder_date"  
#        * "dateparser_date"  
#        * "parsedatetime_date"  
#        * "regex_date"  
#   - "consensus_date": the best guess combining the above.  
#  
# USAGE:  
# 1. Ensure pandas and the necessary date parsing libraries are installed.  
# 2. Import this script.  
# 3. Prepare your DataFrame, ensuring you specify which column contains the filenames or text for extraction.  
# 4. Call the `process_chunk` function with your DataFrame and the column name:  
#  
#     result_df = process_chunk(your_dataframe, 'YourFilenameColumn')  
#  
# 5. The output DataFrame will include all original fields and the new extraction/date columns.  
#  
# EXAMPLE:  
#   Input:  "Analysis_Contracts-11-05.pdf"  
#   Extraction:  
#      "Evaluated string element" → "11-01-05"  
#      "datefinder_date"          → "2005-11-01"  
#      ...  
#  
# CAVEATS:  
# - If no 2- or 3-part numeric group is found, the evaluated string element is blank.  
# - Only the rightmost date-like pattern is extracted, even if others exist in the string.  
# - The "01" insertion heuristic assumes single-part dates represent the first day of the month.  
#  
# -------------------------------------------------------------------------------  
