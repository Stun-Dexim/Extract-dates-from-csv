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

import sys  
import os  
import pandas as pd  
import datefinder  
import dateparser  
import parsedatetime  
import re  
from datetime import datetime  
from collections import Counter  
from dateutil import parser as dateutil_parser  
  
DELIMS = r'[-_/\\\.]'  
DELIM_LIST = ['-', '_', '/', '\\', '.']  
  
def plausible_year(y):  
    try:  
        y = int(y)  
        return 1980 <= y <= 2039  
    except Exception:  
        return False  
  
def extract_same_delim_group_from_reversed(text, num_groups=3):  

    if not isinstance(text, str):  
        return '', '', 0  
    rev_text = text[::-1]  
    for delim in DELIM_LIST:  
        regex_delim = re.escape(delim)  
        patterns = [  
            (3, rf'(\d{{1,2}}{regex_delim}\d{{1,2}}{regex_delim}\d{{2,4}})'),  
            (3, rf'(\d{{2,4}}{regex_delim}\d{{1,2}}{regex_delim}\d{{1,2}})'),  
            (2, rf'(\d{{1,2}}{regex_delim}\d{{2,4}})'),  
            (2, rf'(\d{{2,4}}{regex_delim}\d{{1,2}})'),  
        ]  
        for n_groups, pattern in patterns:  
            for m in re.finditer(pattern, rev_text):  
                rev_candidate = m.group(1)  
                start_idx = m.start(1)  
                end_idx = m.end(1)  
                before = rev_text[start_idx-1] if start_idx > 0 else ''  
                after = rev_text[end_idx] if end_idx < len(rev_text) else ''  
                if (start_idx == 0 or not before.isdigit()) and (end_idx == len(rev_text) or not after.isdigit()):  
                    # Reverse back to original orientation for plausibility  
                    orig_candidate = rev_candidate[::-1]  
                    split_groups = orig_candidate.split(delim)  
                    split_groups = [g for g in split_groups if g != '']  
                    if len(split_groups) == n_groups and ' ' not in orig_candidate:  
                        ok = True  
                        if n_groups == 3:  
                            # Try MM-DD-YY  
                            try:  
                                mm, dd, yy = map(str.strip, split_groups)  
                                mm = int(mm)  
                                dd = int(dd)  
                                yy = int(yy)  
                                if yy < 100:  
                                    # Try both centuries for plausibility  
                                    ok = False  
                                    for century in [1900, 2000]:  
                                        test_yy = yy + century  
                                        if plausible_year(test_yy):  
                                            yy = test_yy  
                                            ok = True  
                                            break  
                                if not (1 <= mm <= 12 and 1 <= dd <= 31 and plausible_year(yy)):  
                                    ok = False  
                            except Exception:  
                                ok = False  
                            # Try YY-MM-DD if above fails  
                            if not ok:  
                                try:  
                                    yy, mm, dd = map(str.strip, split_groups)  
                                    yy = int(yy)  
                                    mm = int(mm)  
                                    dd = int(dd)  
                                    if yy < 100:  
                                        ok = False  
                                        for century in [1900, 2000]:  
                                            test_yy = yy + century  
                                            if plausible_year(test_yy):  
                                                yy = test_yy  
                                                ok = True  
                                                break  
                                    if 1 <= mm <= 12 and 1 <= dd <= 31 and plausible_year(yy):  
                                        ok = True  
                                    else:  
                                        ok = False  
                                except Exception:  
                                    ok = False  
                        elif n_groups == 2:  
                            try:  
                                g1, g2 = map(str.strip, split_groups)  
                                mm_first = (1 <= len(g1) <= 2 and g1.isdigit())  
                                yy_first = ((len(g1) == 2 or len(g1) == 4) and g1.isdigit())  
                                mm_second = (1 <= len(g2) <= 2 and g2.isdigit())  
                                yy_second = ((len(g2) == 2 or len(g2) == 4) and g2.isdigit())  
                                ok = False  
                                # MM-YYYY  
                                if mm_first and yy_second:  
                                    mm = int(g1)  
                                    yy = int(g2)  
                                    if yy < 100:  
                                        for century in [1900, 2000]:  
                                            test_yy = yy + century  
                                            if plausible_year(test_yy):  
                                                yy = test_yy  
                                                break  
                                    if 1 <= mm <= 12 and plausible_year(yy):  
                                        ok = True  
                                # YYYY-MM  
                                elif yy_first and mm_second:  
                                    yy = int(g1)  
                                    mm = int(g2)  
                                    if yy < 100:  
                                        for century in [1900, 2000]:  
                                            test_yy = yy + century  
                                            if plausible_year(test_yy):  
                                                yy = test_yy  
                                                break  
                                    if 1 <= mm <= 12 and plausible_year(yy):  
                                        ok = True  
                            except Exception:  
                                ok = False  
                        # Check no foreign delimiters  
                        if ok:  
                            other_delims = [d for d in DELIM_LIST if d != delim]  
                            found_other = any(d in orig_candidate for d in other_delims)  
                            if not found_other:  
                                return orig_candidate, delim, n_groups  
    return '', '', 0  
  
def extract_rightmost_pattern(text):  
    group, delim, n = extract_same_delim_group_from_reversed(text, 3)  
    if group:  
        return group, 3  
    group, delim, n = extract_same_delim_group_from_reversed(text, 2)  
    if group:  
        # If delim wasn't set, default to '-' for safety  
        delim = delim if delim else '-'  
        parts = group.split(delim)  
        if len(parts) == 2:  
            new_group = f"{parts[0]}{delim}01{delim}{parts[1]}"  
            return new_group, 2  
    return '', 0  
  
def standardize_date(date_str):  
    if not date_str or not isinstance(date_str, str):  
        return ''  
    date_str = date_str.strip()  
    if re.fullmatch(r'\d{4}', date_str):  
        year = int(date_str)  
        if plausible_year(year):  
            return f"{year:04d}-01-01"  
        else:  
            return ''  
    m = re.fullmatch(rf'(\d{{1,2}}){DELIMS}(\d{{2,4}})', date_str)  
    if m:  
        mm, yy = m.groups()  
        mm = int(mm)  
        yy = int(yy)  
        if yy < 100:  
            for century in [1900, 2000]:  
                test_yy = yy + century  
                if plausible_year(test_yy):  
                    yy = test_yy  
                    break  
        if 1 <= mm <= 12 and plausible_year(yy):  
            return f"{yy:04d}-{mm:02d}-01"  
        else:  
            return ''  
    m = re.fullmatch(rf'(\d{{4}}){DELIMS}(\d{{1,2}})', date_str)  
    if m:  
        yy, mm = m.groups()  
        yy = int(yy)  
        mm = int(mm)  
        if yy < 100:  
            for century in [1900, 2000]:  
                test_yy = yy + century  
                if plausible_year(test_yy):  
                    yy = test_yy  
                    break  
        if 1 <= mm <= 12 and plausible_year(yy):  
            return f"{yy:04d}-{mm:02d}-01"  
        else:  
            return ''  
    m = re.fullmatch(rf'(\d{{1,2}}){DELIMS}(\d{{1,2}}){DELIMS}(\d{{2,4}})', date_str)  
    if m:  
        g1, g2, g3 = m.groups()  
        mm, dd, yy = int(g1), int(g2), int(g3)  
        if yy < 100:  
            for century in [1900, 2000]:  
                test_yy = yy + century  
                if plausible_year(test_yy):  
                    yy = test_yy  
                    break  
        # Try MM-DD-YY  
        if 1 <= mm <= 12 and 1 <= dd <= 31 and plausible_year(yy):  
            return f"{yy:04d}-{mm:02d}-{dd:02d}"  
        # Try DD-MM-YY (swap)  
        if 1 <= dd <= 12 and 1 <= mm <= 31 and plausible_year(yy):  
            return f"{yy:04d}-{dd:02d}-{mm:02d}"  
        return ''  
    if not re.search(r'\d{4}', date_str) and not re.search(r'\d{2}', date_str):  
        return ''  
    try:  
        dt = dateutil_parser.parse(date_str, fuzzy=True, default=datetime(2000, 1, 1))  
        if plausible_year(dt.year):  
            return dt.strftime('%Y-%m-%d')  
        else:  
            return ''  
    except Exception:  
        return ''  
  
def extract_date_datefinder(text):  
    try:  
        matches = list(datefinder.find_dates(text))  
        for m in matches:  
            iso = standardize_date(m.date().isoformat())  
            if iso:  
                return iso  
    except Exception:  
        pass  
    return ''  
  
def extract_date_dateparser(text):  
    try:  
        dt = dateparser.parse(text)  
        if dt:  
            return standardize_date(dt.date().isoformat())  
    except Exception:  
        pass  
    return ''  
  
def extract_date_parsedatetime(text):  
    try:  
        cal = parsedatetime.Calendar()  
        time_struct, parse_status = cal.parse(text)  
        if parse_status > 0:  
            dt = datetime(*time_struct[:6])  
            return standardize_date(dt.date().isoformat())  
    except Exception:  
        pass  
    return ''  
  
def extract_date_regex_datetime(text):  
    patterns = [  
        rf'(\d{{4}}){DELIMS}(\d{{1,2}}){DELIMS}(\d{{1,2}})',  
        rf'(\d{{1,2}}){DELIMS}(\d{{1,2}}){DELIMS}(\d{{2,4}})',  
        rf'(\d{{1,2}}){DELIMS}(\d{{2,4}})',  
        rf'(\d{{2,4}}){DELIMS}(\d{{1,2}})',  
        rf'(\d{{4}})',  
    ]  
    for pat in patterns:  
        for m in re.finditer(pat, text):  
            groups = m.groups()  
            try:  
                if len(groups) == 3:  
                    if len(groups[0]) == 4:  
                        year, month, day = int(groups[0]), int(groups[1]), int(groups[2])  
                        if 1 <= month <= 12 and 1 <= day <= 31 and plausible_year(year):  
                            dt = datetime(year, month, day)  
                            return standardize_date(dt.date().isoformat())  
                    elif len(groups[2]) == 4:  
                        year, month, day = int(groups[2]), int(groups[0]), int(groups[1])  
                        if 1 <= month <= 12 and 1 <= day <= 31 and plausible_year(year):  
                            dt = datetime(year, month, day)  
                            return standardize_date(dt.date().isoformat())  
                    elif len(groups[2]) == 2:  
                        year = int(groups[2])  
                        for century in [1900, 2000]:  
                            test_yy = year + century  
                            if plausible_year(test_yy):  
                                year = test_yy  
                                break  
                        month, day = int(groups[0]), int(groups[1])  
                        if 1 <= month <= 12 and 1 <= day <= 31 and plausible_year(year):  
                            dt = datetime(year, month, day)  
                            return standardize_date(dt.date().isoformat())  
                elif len(groups) == 2:  
                    g1, g2 = groups  
                    mm_first = (1 <= len(g1) <= 2 and g1.isdigit())  
                    yy_first = ((len(g1) == 2 or len(g1) == 4) and g1.isdigit())  
                    mm_second = (1 <= len(g2) <= 2 and g2.isdigit())  
                    yy_second = ((len(g2) == 2 or len(g2) == 4) and g2.isdigit())  
                    # MM-YYYY  
                    if mm_first and yy_second:  
                        mm = int(g1)  
                        yy = int(g2)  
                        if yy < 100:  
                            for century in [1900, 2000]:  
                                test_yy = yy + century  
                                if plausible_year(test_yy):  
                                    yy = test_yy  
                                    break  
                        if 1 <= mm <= 12 and plausible_year(yy):  
                            return f"{yy:04d}-{mm:02d}-01"  
                    # YYYY-MM  
                    elif yy_first and mm_second:  
                        yy = int(g1)  
                        mm = int(g2)  
                        if yy < 100:  
                            for century in [1900, 2000]:  
                                test_yy = yy + century  
                                if plausible_year(test_yy):  
                                    yy = test_yy  
                                    break  
                        if 1 <= mm <= 12 and plausible_year(yy):  
                            return f"{yy:04d}-{mm:02d}-01"  
                elif len(groups) == 1:  
                    yy = int(groups[0])  
                    if plausible_year(yy):  
                        return f"{yy:04d}-01-01"  
            except Exception:  
                continue  
    return ''  
  
def consensus_date(dates, two_group=False):  
    iso_dates = []  
    for d in dates:  
        if not d or not isinstance(d, str):  
            continue  
        d = d.strip()  
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', d):  
            iso_dates.append(d)  
    if not iso_dates:  
        return ''  
    count = Counter(iso_dates)  
    most_common = count.most_common(1)[0][0]  
    if two_group:  
        parts = most_common.split('-')  
        if len(parts) == 3:  
            return f"{parts[0]}-{parts[1]}-01"  
    return most_common  
  
def process_chunk(df, text_col):  
    evaluated_elements = []  
    datefinder_results = []  
    dateparser_results = []  
    parsedatetime_results = []  
    regex_results = []  
    consensus_results = []  
  
    for idx, row in df.iterrows():  
        text = row[text_col]  
  
        # Extract patterns for 3-group and 2-group  
        group3, delim3, n3 = extract_same_delim_group_from_reversed(text, 3)  
        splitcount3 = len(group3.split(delim3)) if group3 and delim3 else 0  
  
        group2, delim2, n2 = extract_same_delim_group_from_reversed(text, 2)  
        splitcount2 = len(group2.split(delim2)) if group2 and delim2 else 0  
  
        # Use only *real* 3groups; otherwise transform the 2-group  
        if group3 and delim3 and splitcount3 == 3:  
            final_eval = group3  
        elif group2 and delim2 and splitcount2 == 2:  
            parts = group2.split(delim2)  
            final_eval = f"{parts[0]}{delim2}01{delim2}{parts[1]}"  
        else:  
            final_eval = ""  
  
        evaluated_elements.append(final_eval)  
  
        # Extract and standardize dates  
        d1 = extract_date_datefinder(final_eval)  
        d2 = extract_date_dateparser(final_eval)  
        d3 = extract_date_parsedatetime(final_eval)  
        d4 = extract_date_regex_datetime(final_eval)  
  
        datefinder_results.append(standardize_date(d1))  
        dateparser_results.append(standardize_date(d2))  
        parsedatetime_results.append(standardize_date(d3))  
        regex_results.append(standardize_date(d4))  
        # Two-group in consensus if chosen logic provided  
        is_two_group = (group2 and delim2 and splitcount2 == 2) and not (group3 and delim3 and splitcount3 == 3)  
        consensus_results.append(consensus_date([  
            standardize_date(d1),  
            standardize_date(d2),  
            standardize_date(d3),  
            standardize_date(d4)  
        ], two_group=is_two_group))  
  
    # Output dataframe with only your desired fields  
    result_df = df.copy()  
    result_df['Evaluated string element'] = evaluated_elements  
    result_df['datefinder_date'] = datefinder_results  
    result_df['dateparser_date'] = dateparser_results  
    result_df['parsedatetime_date'] = parsedatetime_results  
    result_df['regex_date'] = regex_results  
    result_df['consensus_date'] = consensus_results  
  
    output_columns = list(df.columns) + [  
        'Evaluated string element',  
        'datefinder_date',  
        'dateparser_date',  
        'parsedatetime_date',  
        'regex_date',  
        'consensus_date'  
    ]  
    return result_df[output_columns]   
  
def main():  
    if len(sys.argv) < 2:  
        print("Usage: python extract_dates_from_csv.py inputfile.csv")  
        return  
    inputfile = sys.argv[1]  
    if not os.path.isfile(inputfile):  
        print(f"File not found: {inputfile}")  
        return  
    base, ext = os.path.splitext(inputfile)  
    outputfile = f"{base}_DateExtractionResults.csv"  
    first_chunk = True  
    chunk_size = 100  
    for chunk in pd.read_csv(inputfile, dtype=str, chunksize=chunk_size):  
        chunk = chunk.fillna('')  
        text_col = chunk.columns[-1]  
        result_chunk = process_chunk(chunk, text_col)  
        result_chunk.to_csv(outputfile, mode='w' if first_chunk else 'a', index=False, header=first_chunk)  
        print(f"Wrote {len(result_chunk)} rows to {outputfile}...")  
        first_chunk = False  
    print(f"All done. Results written to {outputfile}")  
  
if __name__ == "__main__":  
    main()  
