import pandas as pd
import os
import re
import datetime
import matplotlib.pyplot as plt

start = datetime.datetime.now()

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
oracle_source = os.path.join(ROOT_PATH, '20240221_ADDRESSES_BE_UPDATE.csv')
datacloud_target = os.path.join(ROOT_PATH, '20240221_ADDRESSES_BE_UPDATE_S3.csv')
# oracle_source = os.path.join(ROOT_PATH, '20240221_ADDRESSES_BE_UPDATE.csv')
# datacloud_target = os.path.join(ROOT_PATH, '20240221_ADDRESSES_BE_UPDATE_S3.csv')

delimiter = '|'
primary_key = 'CS_COMPANY_ID'

chunk_size = 1000000  # Adjust the chunk size as needed


def read_csv_with_chunks(file_path, delimiter, encoding, result_container):
    df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, on_bad_lines='skip', index_col=False)
    result_container.append(df)


def read_and_merge_csv(file_path, delimiter, encoding):
    chunks = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, on_bad_lines='skip', index_col=False,
                         chunksize=chunk_size, engine='python')
    merged_df = pd.concat(chunks, ignore_index=True)
    merged_df.sort_values(by=primary_key, inplace=True)
    return merged_df


# ------------------------------------- FUNCTIONS DECLARED BY ME -------------------------------------------------------

def detect_encoding2(file_path, encodings_list, num_rows=5):
    try:
        with open(file_path, 'rb') as f:
            rawdata = b''.join(f.readline() for _ in range(num_rows))
            return next((encoding for encoding in encodings_list if
                         all((decoded_text := rawdata.decode(encoding)) for _ in range(num_rows))), None)
    except Exception as e:
        print("Error occurred while detecting encoding:", e)
        return None


encodings_list = ["ANSI", "ISO-8859-1", "Windows-1251", "Windows-1252", "GB2312", "Shift JIS", "EUC-KR",
                  "ISO-8859-9", "Windows-1254", "EUC-JP", "Big5"]
print('---------------------------------------------------------------')


# --------------------------------------------------------------------------

# Function to get the delimiter of a file
def get_file_delimiter(file_path, encoding, check_lines=3):
    delimiters = [',', '\t', ';', '|*|', '|', ':', ' ', '~', '`', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-',
                  '_', '=', '+', '[', ']', '{', '}', '\\', '/', '<', '>', '?']
    results = {delimiter: 0 for delimiter in delimiters}
    with open(file_path, 'r', encoding=encoding) as file:
        [results.update({delimiter: results[delimiter] + line.count(delimiter)}) for _ in range(check_lines) if
         (line := file.readline())]
    return max(results, key=results.get)

# if delimiter = |*|, it converts to r'\|*\|'
def convert_delimiter_to_raw(delimiter):
    """
    Convert delimiter to raw string literal character type.
    Parameters:
        delimiter (str): Delimiter string
    Returns:
        str: Raw string literal character type delimiter
    """
    raw_delimiter = re.escape(delimiter)
    return rf'{raw_delimiter}'

# --------------------------------------------------------------------------
# Function to detect the extension of a file
def detect_extension(file_path):
    _, extension = os.path.splitext(file_path)
    return extension.lower()

# 3m x 3m
# ------------------------------------- FUNCTIONS DECLARED BY ME -------------------------------------------------------

extension1 = detect_extension(oracle_source)
extension2 = detect_extension(datacloud_target)

encoding1 = detect_encoding2(oracle_source, encodings_list)
encoding2 = detect_encoding2(datacloud_target, encodings_list)

delimiter1 = get_file_delimiter(oracle_source, encoding1)
delimiter2 = get_file_delimiter(datacloud_target, encoding2)

if (delimiter1 != delimiter2):
    print('Delimiters of both files dont match')
    print('Delimiter of Oracle Source: ', delimiter1)
    print('Delimiter of Datacloud Target: ', delimiter2)
else:
    print('Delimiters Matched. i.e. Delimiter = ', delimiter1)

if encoding1 != encoding2:
    print('Encodings of both files do not match')
    print('Encoding of Oracle Source: ', encoding1)
    print('Encoding of Datacloud Target: ', encoding2)
else:
    print('Encodings Matched. i.e. Encoding =', encoding1)

print('---------------------------------------------------------------')

# GIL --> Global interpreter lock

delimiter1 = convert_delimiter_to_raw(delimiter1)
delimiter2 = convert_delimiter_to_raw(delimiter2)

df1 = read_and_merge_csv(oracle_source, delimiter1, encoding1)
df2 = read_and_merge_csv(datacloud_target, delimiter2, encoding2)

num_records_oracle_source = len(df1)
num_records_datacloud_target = len(df2)

# Get the number of unique records for the given primary_key in each file
num_unique_records_oracle_source = df1[primary_key].nunique()
num_unique_records_datacloud_target = df2[primary_key].nunique()

col1 = list(df1.columns)
col2 = list(df2.columns)

# Get the number of matching and unmatching records in both files
matching_records = pd.merge(df1, df2, on=col1, how='inner')
num_matching_records = len(matching_records)
unmatching_records_in_oracle_source = pd.concat([df1, matching_records]).drop_duplicates(keep=False)
unmatching_records_in_datacloud_target = pd.concat([df2, matching_records]).drop_duplicates(keep=False)
num_unmatching_records = len(unmatching_records_in_oracle_source) + len(unmatching_records_in_datacloud_target)

# sample contains total unmatching records
sample = pd.merge(unmatching_records_in_oracle_source, unmatching_records_in_datacloud_target, on=[primary_key],
                  how='inner')

# Finding records from unmatching_records_in_oracle_source for not matching primary key in df1
oracle_only_records = unmatching_records_in_oracle_source[
    ~unmatching_records_in_oracle_source[primary_key].isin(sample[primary_key])]

# Finding records from unmatching_records_in_datacloud_target for not matching primary key in df2
datacloud_only_records = unmatching_records_in_datacloud_target[
    ~unmatching_records_in_datacloud_target[primary_key].isin(sample[primary_key])]

# Difference Data
matching_for_safenumber_oracle_source = unmatching_records_in_oracle_source[
    unmatching_records_in_oracle_source[primary_key].isin(sample[primary_key])]
matching_for_safenumber_datacloud_target = unmatching_records_in_datacloud_target[
    unmatching_records_in_datacloud_target[primary_key].isin(sample[primary_key])]
data_mismatch_records = pd.merge(matching_for_safenumber_oracle_source, matching_for_safenumber_datacloud_target,
                                 on=[primary_key], how='inner')

column_details = df1.columns
data_mismatch_records = data_mismatch_records.fillna('')

unmatched_data = pd.DataFrame(columns=df1.columns)

for index, data in data_mismatch_records.iterrows():
    for column_name in column_details:
        if column_name == primary_key:
            unmatched_data.at[index, column_name] = data[column_name]
        elif data[column_name + '_x'] != data[column_name + '_y']:
            updated_column_value = str(data[column_name + '_x']) + ' -> ' + str(data[column_name + '_y'])
            unmatched_data.at[index, column_name] = updated_column_value
        else:
            unmatched_data.at[index, column_name] = data[column_name + '_x']

# Saving the output to a csv file
unmatched_data.to_csv(os.path.join(ROOT_PATH, 'unmatched_data.csv'), index=False)
oracle_only_records.to_csv(os.path.join(ROOT_PATH, 'oracle_only_records.csv'), index=False)
datacloud_only_records.to_csv(os.path.join(ROOT_PATH, 'datacloud_only_records.csv'), index=False)

print(f'Total Processing time - {datetime.datetime.now() - start}')

# Get the number of primary_key which are present/not present in both files
present_in_both = df1[df1[primary_key].isin(df2[primary_key])]
not_present_in_both = df1[~df1[primary_key].isin(df2[primary_key])]

# For a primary_key which is present in both files, if other values are not matching,
# generate a result showing which column is not matching
mismatched_records = matching_records[matching_records[df1.columns] != matching_records[df2.columns]]
mismatched_columns = mismatched_records.columns[mismatched_records.isnull().any()].tolist()

# Create a bar plot
labels = ['oracle_record_count', 'datacloud_record_count', 'oracle_unique_records', 'datacloud_unique_records',
          'matched_records', 'unmatched_records'][::-1]
values = [num_records_oracle_source, num_records_datacloud_target, num_unique_records_oracle_source,
          num_unique_records_datacloud_target, num_matching_records, num_unmatching_records][::-1]

# Set a larger figure size to avoid text trimming
plt.figure(figsize=(10, 6))

plt.barh(labels, values)
plt.xlabel('Metrics')
plt.ylabel('Count')

# Display the number inside each bar
for i, v in enumerate(values):
    plt.text(v, i, str(v), ha='left', va='center')

plt.title('Comparison of Metrics')

# Adjust layout to prevent text trimming
plt.tight_layout()

# Save the plot to a file
plot_output_file_path = os.path.join(ROOT_PATH, 'metrics_comparison_plot.png')
plt.savefig(plot_output_file_path)

# Show the plot
plt.show()

output_file_path = 'output.txt'

with open(output_file_path, 'w') as output_file:
    print(f"Number of records in oracle: {num_records_oracle_source}", file=output_file)
    print(f"Number of records in datacloud: {num_records_datacloud_target}", file=output_file)
    print(f"Number of unique records in oracle: {num_unique_records_oracle_source}", file=output_file)
    print(f"Number of unique records in datacloud: {num_unique_records_datacloud_target}", file=output_file)
    print(f"Number of matched records: {num_matching_records}", file=output_file)
    print(f"Number of unmatched records: {num_unmatching_records}", file=output_file)
    print(f"oracle only records: {len(oracle_only_records)}", file=output_file)
    print(f"datacloud only records: {len(datacloud_only_records)}", file=output_file)
    print(f"Number of {primary_key} present in both files: {len(present_in_both)}", file=output_file)
    print(f"Number of {primary_key} not present in both files: {len(not_present_in_both)}", file=output_file)
    print(f"Mismatched columns for {primary_key} present in both files: {mismatched_columns}", file=output_file)

# ----------------------------------------------SCENARIOS--------------------------------------------------------------
print()

# ============================================== SCENARIO 1 ===========================================================
# 1.Verify that the number of records in the Oracle file matches the number of records in the cloud file.

# print(df1.shape, df2.shape)
num_rows_df1, num_cols_df1 = df1.shape
num_rows_df2, num_cols_df2 = df2.shape


def num_records_in_source_match_num_rec_in_target1(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario1: num_records_in_source_match_num_rec_in_target1\n')
        output_file.write(
            f"num records in oracle = num records in datacloud S3 = {num_cols_df1}\n" if num_rows_df1 == num_rows_df2 else f"num records in oracle: {num_rows_df1}\nnum records in Datacloud: {num_rows_df2}\n")


num_records_in_source_match_num_rec_in_target1(output_file_path)


# ============================================== SCENARIO 2 ===========================================================
# 2.Validate that the column names in both files are identical and in the same order.
def compare_column_names_and_order(output_file_path, col1, col2):
    with open(output_file_path, 'a') as output_file:
        output = "\nScenario2: Comparing Column Names and Order:\n"
        output += (
            'Column names are identical but not in the same order\n' if col1 != col2 and sorted(col1) == sorted(col2)
            else 'Column names are not identical\n' if col1 != col2
            else 'Column names are identical & in the same order\n')
        output_file.write(output)


compare_column_names_and_order(output_file_path, col1, col2)

# ============================================== SCENARIO 3 ===========================================================
# 3.Confirm that the data types of all the columns in both files match.
type1 = list(df1.dtypes)
type2 = list(df2.dtypes)


def compare_data_types_between_dataframes2(output_file_path, type1, type2):
    datetime_col_idx_df1 = next((index for index, dtype in enumerate(type1) if dtype == 'datetime64[ns]'), -1)
    datetime_col_idx_df2 = next((index for index, dtype in enumerate(type2) if dtype == 'datetime64[ns]'), -1)
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario 3: Comparing Data Types Between DataFrames\n')
        output_file.write('Data types of all columns in both files matched successfully\n' if sorted(type1) == sorted(
            type2) else 'Data types of Columns in both files don\'t match\n' if len(type1) == len(
            type2) else 'Number of columns in both files don\'t match, Hence can\'t compare data types\n')
    return datetime_col_idx_df1, datetime_col_idx_df2


datetime_col_idx_df1, datetime_col_idx_df2 = compare_data_types_between_dataframes2(output_file_path, type1, type2)


# ============================================== SCENARIO 4 ===========================================================
# 4.Check for any differences in the data between the two files by comparing the values of each column in both files,
#   meets the required business rules, such as data constraints, data dependencies, and data formatting...


# ============================================== SCENARIO 5 ===========================================================
def check_datetime_format_match(df1, df2, datetime_col_idx_df1, datetime_col_idx_df2, output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write(
            "\nScenario 5: Verify that any date or timestamp values in both files are in the same format.\n")

        if datetime_col_idx_df1 == -1 or datetime_col_idx_df2 == -1:
            output_file.write("Dataframes dont contain Datetime column\n")
            return None, None
        datetime_col_df1, datetime_col_df2 = df1.iloc[:, datetime_col_idx_df1], df2.iloc[:, datetime_col_idx_df2]
        datetime_col_df1, datetime_col_df2 = pd.to_datetime(datetime_col_df1, errors='coerce'), pd.to_datetime(
            datetime_col_df2, errors='coerce')
        formatt_df1, formatt_df2 = datetime_col_df1.dt.strftime('').iloc[0], datetime_col_df2.dt.strftime('').iloc[0]
        format_match = formatt_df1 == formatt_df2
        output_file.write(f"Both files datetime formats: {formatt_df1}\n" if format_match else f"Formats don't match.\n")
        output_file.write(f"Datetime format for df1: {formatt_df1}\n")
        output_file.write(f"Datetime format for df2: {formatt_df2}\n")
    return formatt_df1, formatt_df2


format_df1, format_df2 = check_datetime_format_match(df1, df2, datetime_col_idx_df1, datetime_col_idx_df2,
                                                     output_file_path)


# ============================================== SCENARIO 6 ===========================================================
# 6.Check for any null values in both files, and ensure that they match.
def compare_null_values_between_dataframes(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario6 : Check for any null values in both files, and ensure that they match.\n')
        null_count_df1 = df1.isnull().sum()  # count of null values
        null_count_df2 = df2.isnull().sum()
        if null_count_df1.equals(null_count_df2):
            output_file.write('Null values in both files match successfully!\n')
        else:
            output_file.write('Null values in both files don\'t match\n')
            output_file.write(f'Null count in df1:\n{null_count_df1}\n\n')
            output_file.write(f'Null count in df2:\n{null_count_df2}\n')


compare_null_values_between_dataframes(output_file_path)


# ============================================== SCENARIO 7 ===========================================================
# 7.Validate that any calculations or aggregations performed on the data in both files produce identical results.


# ============================================== SCENARIO 8 ===========================================================
# 8.Verify that any joins or relationships between tables in both files produce identical results.


# ============================================== SCENARIO 9 ===========================================================
# 9.Check for any differences in the encoding between the two files.
def compare_encodings_between_files(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario9 compare_encodings_between_files:\n')
        encoding_df1, encoding_df2 = detect_encoding2(oracle_source, encodings_list), detect_encoding2(datacloud_target,
                                                                                                       encodings_list)
        output_file.write(
            f'Encoding for both files is same, i.e.: {encoding_df2}\n' if encoding_df1 == encoding_df2 else 'Both files have different encodings\n')


compare_encodings_between_files(output_file_path)


# ============================================== SCENARIO 10 ===========================================================
# 10.Confirm that the data in both files is sorted in the same order, if applicable.

def confirm_data_order_similarity(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario10: confirm_data_order_similarity.\n')
        output_file.write((
                              "The data in both files is sorted in the same order.\n" if num_rows_df1 == num_rows_df2 and num_cols_df1 == num_rows_df2 and df1.equals(
                                  df2) else "The data in both files is not sorted in the same order.\n") if num_rows_df1 == num_rows_df2 and num_cols_df1 == num_rows_df2 else 'Both files differ in size, hence can\'t compare the order\n' + f'Shape of oracle source: {df1.shape}\n' + f'Shape of datacloud target: {df2.shape}\n')


confirm_data_order_similarity(output_file_path)

# ============================================== SCENARIO 11 ===========================================================
# 11.Test both files with a significant amount of data.


# ============================================== SCENARIO 12 ===========================================================
# 12. Verify if there any duplicates present in the data due to change in the generation process.
num_dup_df1 = df1.duplicated().sum()
num_dup_df2 = df2.duplicated().sum()


def check_duplicate_records(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario12 check_duplicate_records:\n')
        output_file.write(f"Count of duplicate records in df1: {num_dup_df1}\n")
        output_file.write(f"Count of duplicate records in df2: {num_dup_df2}\n")


check_duplicate_records(output_file_path)


# ============================================== SCENARIO 13 ===========================================================
# 13. verify unique records in source
def unique_records_in_source(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario13: unique records in source\n')
        output_file.write(f'Unique records in oracle source: {num_unique_records_oracle_source}\n')


unique_records_in_source(output_file_path)


# ============================================== SCENARIO 14 ===========================================================
# 14. verify unique records in target
def unique_records_in_target(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario14: Unique records in target\n')
        output_file.write(f'Unique records in datacloud target: {num_unique_records_datacloud_target}\n')


unique_records_in_target(output_file_path)


# ============================================== SCENARIO 15 ===========================================================
# 15.Duplicates in Source
def check_duplicate_records_in_source(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario15: check_duplicate_records_in_source\n')
        output_file.write(
            f'There are {num_dup_df1} duplicates in oracle source\n' if num_dup_df1 > 0 else f'There are No Duplicates in oracle source, i.e. {num_dup_df1}\n')


check_duplicate_records_in_source(output_file_path)


# ============================================== SCENARIO 16 ===========================================================
# 16. Duplicates in Target
def check_duplicate_records_in_target(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write('\nScenario16: check_duplicate_records_in_target\n')
        output_file.write(
            f'There are {num_dup_df2} duplicates in Datacloud target\n' if num_dup_df2 > 0 else f'There are No Duplicates in Datacloud Target, i.e. {num_dup_df2}\n')


check_duplicate_records_in_target(output_file_path)


# ============================================== SCENARIO 17 ===========================================================
# 17.Total matched records
def total_matched_records(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write(f"\n17: Matched records len: {len(matching_records)}\n")


total_matched_records(output_file_path)


# ============================================== SCENARIO 18 ===========================================================
# 18.Total unmatched records
def print_unmatched_records(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write("\n18: Unmatched records len: {}\n".format(len(unmatched_data)))


print_unmatched_records(output_file_path)


# ============================================== SCENARIO 19 ===========================================================
# 19.Records present in Source only
def print_oracle_only_records(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write("\n19: Oracle only records len: {}\n".format(len(oracle_only_records)))


print_oracle_only_records(output_file_path)


# ============================================== SCENARIO 20 ===========================================================
# 20.Records present in target only
def print_datacloud_only_records(output_file_path):
    with open(output_file_path, 'a') as output_file:
        output_file.write("\n20: datacloud_only_records len {}\n".format(len(datacloud_only_records)))


print_datacloud_only_records(output_file_path)
