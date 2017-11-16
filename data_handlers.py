import csv


# processing csv file (function name is <file_extension>_handler)
def csv_handler(content):
    rows = []
    csv_rows = csv.DictReader(content)
    for row in csv_rows:
        rows.append(row)
    return rows


# processing txt file (function name is <file_extension>_handler)
def txt_handler(content):
    # transform content to dictionaries
    # ...
    # return array of dict
    return []


# processing file of another type (function name is <file_extension>_handler)
def another_type_handler(content):
    # transform content to dictionaries
    # ...
    # return array of dict
    return []
