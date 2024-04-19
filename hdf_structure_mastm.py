#!/usr/bin/env python3.11
import pandas as pd
import argparse
import sys
import h5py
import codecs

def append_data_to_hdf5(hdf5_path, group_name, type_name, csv_input, chunksize=1000000):
    reader = pd.read_csv(csv_input, chunksize=chunksize, low_memory=False, index_col=False, dtype=str)
    unique_keys = set()
    column_names = None
    with pd.HDFStore(hdf5_path, mode='a', complevel=9, complib='zlib') as store:
        for chunk in reader:
            if column_names is None:
                column_names = chunk.columns.tolist()
            if type_name == 'complete_nbbo':
                index_position = 2
            if type_name == 'ctm':
                index_position = 3
            if type_name == 'mastm':
                index_position = 1
            for unique_key, group_df in chunk.groupby(chunk.columns[index_position]):
                hdf5_key = f'{unique_key}/{group_name}/{type_name}'
                min_itemsize = {col: 20 for col in group_df.columns}
                if 'SEC_DESC' in group_df.columns:
                    min_itemsize['SEC_DESC'] = 750
                if 'SEC_TYPE' in group_df.columns:
                    min_itemsize['SEC_TYPE'] = 10
                if 'TEST_SYMBOL_FLAG' in group_df.columns:
                    min_itemsize['TEST_SYMBOL_FLAG'] = 5
                if 'LISTED_EXCHANGE' in group_df.columns:
                    min_itemsize['LISTED_EXCHANGE'] = 5
                if 'TAPE' in group_df.columns:
                    min_itemsize['TAPE'] = 5
                if 'UOT' in group_df.columns:
                    min_itemsize['UOT'] = 10
                if 'ROUND_LOT' in group_df.columns:
                    min_itemsize['ROUND_LOT'] = 10
                if 'NYSE_INDUSTRY_CODE' in group_df.columns:
                    min_itemsize['NYSE_INDUSTRY_CODE'] = 10
                if 'HALT_DELAY_REASON' in group_df.columns:
                    min_itemsize['HALT_DELAY_REASON'] = 5
                if 'SPECIALIST_CLEARING_AGENT' in group_df.columns:
                    min_itemsize['SPECIALIST_CLEARING_AGENT'] = 10
                if 'SPECIALIST_CLEARING_NUM' in group_df.columns:
                    min_itemsize['SPECIALIST_CLEARING_NUM'] = 10
                if 'SPECIALIST_POST_NUM' in group_df.columns:
                    min_itemsize['SPECIALIST_POST_NUM'] = 5
                if 'SPECIALIST_PANEL' in group_df.columns:
                    min_itemsize['SPECIALIST_PANEL'] = 5
                for column in group_df.columns:
                    if column.startswith('TRON_'):
                        min_itemsize[column] = 5
                if 'TICK_PILOT_INDICATOR' in group_df.columns:
                    min_itemsize['TICK_PILOT_INDICATOR'] = 5
                store.append(hdf5_key, group_df, format='table', data_columns=True, index=False, min_itemsize=min_itemsize)
                unique_keys.add(hdf5_key)

    with h5py.File(hdf5_path, 'a') as hdf_file:
        for hdf5_key in unique_keys:
            if hdf5_key in hdf_file:
                dset = hdf_file[hdf5_key]
                dset.attrs['column_names'] = column_names


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append CSV data to an HDF5 file, organized by unique values in the fourth column.")
    parser.add_argument("hdf5_path", help="Path to the HDF5 file.")
    parser.add_argument("group_name", help="Name of the group under which data should be stored.")
    parser.add_argument("type_name", help="Type of the data (e.g., ctm, mastm..).")
    parser.add_argument("csv_path", nargs='?', default=sys.stdin, help="Path to the input CSV file or '-' for stdin.")
    args = parser.parse_args()

    if args.csv_path is sys.stdin or args.csv_path == '-':
        if sys.stdin.isatty():
            print("Error: No data piped to script and no CSV file path provided.", file=sys.stderr)
            sys.exit(1)
        else:
            stdin_decoder = codecs.getreader('iso-8859-1')(sys.stdin.buffer)
            append_data_to_hdf5(args.hdf5_path, args.group_name, args.type_name, stdin_decoder)
    else:
        with open(args.csv_path, 'r', encoding='iso-8859-1') as f:
            append_data_to_hdf5(args.hdf5_path, args.group_name, args.type_name, f)
