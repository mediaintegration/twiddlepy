import os, time
import hashlib
import logging
from .config import config
from configparser import InterpolationSyntaxError

try:
    logging_format = config['Logging']['Format']
except InterpolationSyntaxError:
    print('Failed to load logger format from config')
    raise Exception
except KeyError as e:
    print(e)
    raise e

logging.basicConfig(format=logging_format)
logger = logging.getLogger(config['Logging']['Name'])
logger.setLevel(config['Logging']['Level'])

'''
    Function to copy dataframe or dict of dataframes

    Params:
        dfs: dataframe or dict of dataframes

    Return:
        A copy of input param
'''
def df_copy(dfs):
    if isinstance(dfs, dict):
        dfc = {}
        for k, v in dfs.items():
            dfc[k] = v.copy()
        return dfc
    
    return dfs.copy()

'''
    Function to apply a func to a dataframe or dict of dataframes

    Params:
        dfs: dataframe or dict of dataframes

    Return:
        processed dataframe or dict or dataframes
'''
def apply_function(dfs, func):
    mdfs = df_copy(dfs)
    if isinstance(mdfs, dict):
        return {k : func(v) for k, v in mdfs.items()}
    
    return func(mdfs)


'''
    Function to return the age of a file (since last modified time) in seconds
    Params:
        fn -- file path of the file
        ref_time -- reference time, current time if None
    Returns:
        age of file, since last mod, in seconds
'''
def file_age_in_seconds(fn, ref_time=None):
    if ref_time is None:
        ref_time = int(time.time())
    mod_time = os.path.getmtime(fn)
    return ref_time - mod_time
    


'''
    Function to genrate md5 digest

    Params:
        x: input to generate digest for

    Returns:
        hex value of md5 digest
'''
def md5_digest(x):
    return hashlib.md5(str(x).encode('utf-8')).hexdigest()


'''
    Function to concatenate a row

    Params:
        row: row to concatenate

    Returns:
        concatenated row string
'''
def concat_row(row):
    return '-'.join(row.astype(str).values)

'''
    Function function to generate an id column from selected col, col_names

    Params:
        df: dataframe to insert/modify id column for
        col_names: column from which to generate the id value
        idcol: id column
        overwrite: if to overwrite existing id column
        hashing: if to apply hashing to the id column

    Returns:
        
'''
def generate_ids_using_cols(df, col_names, idcol='id', overwrite=False, hashing=True):
    mdf = df.copy()
    if overwrite or idcol not in mdf.columns:
        mdf[idcol] = mdf[col_names].apply(concat_row, axis='columns')
        if (hashing):
            mdf[idcol] = mdf[idcol].apply(md5_digest)
    return mdf


lowercase = lambda x: x.lower()

uppercase = lambda x: x.upper()