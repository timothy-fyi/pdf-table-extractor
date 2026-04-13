import os
import glob
import time
import shutil
import yaml
import camelot
import pandas as pd
import numpy as np

def load_yaml(file, template=None):
    """
    Loads a YAML file with error handling and template control.

    Parameters:
        file (str): File path of file to load
        template (str): Optional. File path of template file if original file doesn't exist.

    """
    if template:
        if not os.path.exists(file):
            try:
                shutil.copy(template, file)
                print('File missing. A new one has been created. You must open the new file and define the variables.')
            except FileNotFoundError:
                print('Specified template file does not exist.')
            exit()
    try:
        with open(file, 'r') as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        print('File not found. Please check path.')
        exit()

def get_files(file_location):
    """
    Returns a list of files from a folder.

    Parameters:
        file_location (str): Folder location of files. Asterisk(*) must be included after folder path to return file paths. 
    """
    print('Getting file(s)...')
    return glob.glob(file_location)

def extract_and_export_tables(file, pages, export_folders, extract_string=False, flavor='stream'):
    """
    Extracts tables from a PDF file and exports them to CSV. Also returns a list of the locations of resulting CSVs.

    Parameters:
        file (str): PDF file to be used. Include full path.
        pages (str): Pages where tables are to be extracted. As string, separated by comma.
        export_folders (str or list): Export location for tables. Input multiple as list.
        extract_string (list): Extracts text from file name. Two numbers only, inputted as list. Example: [-8,-4]
        flavor (str): From camelot. 'stream': infers tables. 'lattice': works when tables are clearly defined in PDF.
    """
    csv_paths = []

    if isinstance(export_folders, str):
        export_folders = [export_folders]

    if isinstance(pages, int):
        pages = str(pages)

    if len(extract_string) > 2:
        print('Argument extract_string contains more numbers than expected. Please input again with two numbers only.')
        exit()

    file_name = os.path.basename(file)
    print(f'Extracting and exporting tables from {file_name} to csv...')

    try:
        tables = camelot.read_pdf(file, flavor=flavor, pages=pages, suppress_stdout=True)
    except IndexError:
        print('Pages not found. Did you input the correct page numbers?')
        exit()

    if len(export_folders) > 1 and len(tables) != len(export_folders):
        raise ValueError('The number of export folders must match the number of tables extracted when more than one folder is provided.')

    for i, table in enumerate(tables):
        if len(export_folders) == 1:
            folder = export_folders[0]
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            new_file_name = f'export_{timestamp}'
        else:
            folder = export_folders[i]
            new_file_name = os.path.basename(os.path.normpath(folder)).lower().replace(' ', '_')

        if extract_string:
            string_extract = file[extract_string[0]:extract_string[1]]
            file_path = os.path.join(folder, f'{new_file_name}{string_extract}.csv')
        else:
            file_path = os.path.join(folder, f'{new_file_name}.csv')

        table.to_csv(file_path)
        csv_paths.append(file_path)

    return csv_paths

def clean_csv(file, clean_start=None, clean_end=None, filter_column=None, filter_values=None):
    if filter_values is None:
        filter_values = []

    if isinstance(file, str):
        file = [file]

    for csv in file:
        print(f"\n--- Cleaning {os.path.basename(csv)} ---")

        try:
            df = pd.read_csv(csv, skip_blank_lines=False, header=None)
        except Exception as e:
            raise RuntimeError(f"[LOAD ERROR] {csv}: {e}")

        if clean_start:
            try:
                # normalize entire DF for searching
                df_clean = df.map(
                    lambda x: str(x).replace('\xa0', ' ').strip().lower()
                    if pd.notna(x) else x
                )

                target = clean_start.strip().lower()

                matches = df_clean[df_clean.isin([target]).any(axis=1)]

                if matches.empty:
                    raise ValueError(
                        f"'{clean_start}' not found.\nPreview:\n{df.head(10)}"
                    )

                header_row = matches.index[0]

                df = df.iloc[header_row:].reset_index(drop=True)
            
                df = df.dropna(axis=1, how='all')

                df.columns = df.iloc[0]
                df = df[1:].reset_index(drop=True)

                # removes rows that are not not needed (before first clean_start)
                def is_valid_data_row(row):
                    val = str(row.get('County', '')).strip()
                    return val and val.lower() not in ['nan', 'change']

                while len(df) > 0 and not is_valid_data_row(df.iloc[0]):
                    df = df.iloc[1:].reset_index(drop=True)

                # clean column names
                df.columns = [
                    str(col).replace('\xa0', ' ').strip()
                    for col in df.columns
                ]

            except Exception as e:
                raise RuntimeError(
                    f"[CLEAN_START FAILURE]\nFile: {csv}\nTarget: {clean_start}\nError: {e}"
                )

        # normalize all cells
        df = df.map(
            lambda x: str(x).replace('\xa0', ' ').strip()
            if isinstance(x, str) else x
        )

        if clean_end:
            try:
                if 'County' not in df.columns:
                    raise KeyError(
                        f"'County' column missing. Columns: {list(df.columns)}"
                    )

                county = df['County'].astype(str).str.strip()

                matches = df[county.str.lower() == clean_end.strip().lower()]

                if matches.empty:
                    raise ValueError(
                        f"'{clean_end}' not found in County column.\n"
                        f"Nearby values:\n{county[county.str.contains('total', case=False, na=False)].unique()}"
                    )

                end_idx = matches.index[0]
                df = df.loc[:end_idx - 1]

            except Exception as e:
                raise RuntimeError(
                    f"[CLEAN_END FAILURE]\nFile: {csv}\nTarget: {clean_end}\nError: {e}"
                )

        if filter_column:
            try:
                if filter_column not in df.columns:
                    raise KeyError(
                        f"Column '{filter_column}' not found.\nColumns: {list(df.columns)}"
                    )

                for value in filter_values:
                    df = df[
                        ~df[filter_column]
                        .astype(str)
                        .str.contains(value, case=False, na=False)
                    ]

            except Exception as e:
                raise RuntimeError(
                    f"[FILTER FAILURE]\nFile: {csv}\nColumn: {filter_column}\nError: {e}"
                )
            
        try:
            df = df.dropna(axis=1, how='all')
            df.to_csv(csv, index=False)
        except Exception as e:
            raise RuntimeError(f"[SAVE FAILURE] {csv}: {e}")

        print(f"Done: {os.path.basename(csv)}")

def move_files(file, grab_from_folder, move_to_folder):
    """
    Moves file(s) to a new location.

    Parameters:
        file (str): File to be moved. Include full path.
        grab_from_folder (str): Folder path where file to be moved is located.
        move_to_folder (str): Folder path to move file to.
    """
    file_name = os.path.basename(file)
    print(f'Moving {file_name} to specified folder...')
    shutil.move(os.path.join(grab_from_folder, file_name), os.path.join(move_to_folder, file_name))