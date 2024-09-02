import hashlib
import re
import multiprocessing
from multiprocessing import Pool

import numpy as np
from fuzzywuzzy import fuzz
import pandas
from pandas import StringDtype


def compile_RE_tuple(re_tuple):
    return (re.compile(re_tuple[0]), re_tuple[1])


prefix_street = r"(.* )"
road_abbr_not_compiled = [
    (prefix_street + "ROAD", r"\1RD"),
    (prefix_street + "ALLEY", r"\1ALY"),
    (prefix_street + "AVENUE", r"\1AVE"),
    (prefix_street + "BOULEVARD", r"\1BLVD"),
    (prefix_street + "CAUSEWAY", r"\1CSWY"),
    (prefix_street + "CENTER", r"\1CTR"),
    (prefix_street + "CIRCLE", r"\1CIR"),
    (prefix_street + "COVE", r"\1CV"),
    (prefix_street + "CROSSING", r"\1XING"),
    (prefix_street + "DRIVE", r"\1DR"),
    (prefix_street + "EXPRESSWAY", r"\1EXPY"),
    (prefix_street + "EXTENSION", r"\1EXT"),
    (prefix_street + "FREEWAY", r"\1FWY"),
    (prefix_street + "GROVE", r"\1GRV"),
    (prefix_street + "HIGHWAY", r"\1HWY"),
    (prefix_street + "HOLLOW", r"\1HOLW"),
    (prefix_street + "JUNCTION", r"\1JCT"),
    (prefix_street + "MOTORWAY", r"\1LN"),
    (prefix_street + "OVERPASS", r"\1OPAS"),
    (prefix_street + "PARKWAY", r"\1PKWY"),
    (prefix_street + "PLACE", r"\1PL"),
    (prefix_street + "PLAZA", r"\1PLZ"),
    (prefix_street + "POINT", r"\1PT"),
    (prefix_street + "ROUTE", r"\1RTE"),
    (prefix_street + "SKYWAY", r"\1SKWY"),
    (prefix_street + "SQUARE", r"\1SQ"),
    (prefix_street + "STREET", r"\1ST"),
    (prefix_street + "TERRACE", r"\1TER"),
    (prefix_street + "TRAIL", r"\1TRL")]
road_abbr = list(map(compile_RE_tuple, road_abbr_not_compiled))

prefix_location = r"(.* )"
postfix_location = r"( .*)"
location_abbr_not_compiled = [
    (prefix_street + "APARTMENT" + postfix_location, r"\1APT\2"),
    (prefix_street + "BASEMENT" + postfix_location, r"\1BSMT\2"),
    (prefix_street + "BUILDING" + postfix_location, r"\1BLDG\2"),
    (prefix_street + "DEPARTMENT" + postfix_location, r"\1DEPT\2"),
    (prefix_street + "FLOOR" + postfix_location, r"\1FL\2"),
    (prefix_street + "HANGER" + postfix_location, r"\1HNGR\2"),
    (prefix_street + "LOBBY" + postfix_location, r"\1LBBY\2"),
    (prefix_street + "LOWER" + postfix_location, r"\1LOWR\2"),
    (prefix_street + "OFFICE" + postfix_location, r"\1OFC\2"),
    (prefix_street + "PENTHOUSE" + postfix_location, r"\1PH\2"),
    (prefix_street + "ROOM" + postfix_location, r"\1RM\2"),
    (prefix_street + "SUITE" + postfix_location, r"\1STE\2"),
    (prefix_street + "TRAILER" + postfix_location, r"\1TRLR\2"),
    (prefix_street + "UPPER" + postfix_location, r"\1UPPR\2")]
location_abbr = list(map(compile_RE_tuple, location_abbr_not_compiled))

prefix_directional = r"(.+ )"
postfix_directional = r"($)"
pre_streetName_directional = r"(.+ )"
post_streetName_directional = r"( rd)"
directional_abbr_not_compiled = [
    (prefix_directional + "WEST" + postfix_directional, r"\1W\2"),
    (prefix_directional + "EAST" + postfix_directional, r"\1E\2"),
    (prefix_directional + "SOUTH" + postfix_directional, r"\1S\2"),
    (prefix_directional + "NORTH" + postfix_directional, r"\1N\2"),
    (prefix_directional + "NORTH WEST" + postfix_directional, r"\1N W\2"),
    (prefix_directional + "NORTH EAST" + postfix_directional, r"\1N E\2"),
    (prefix_directional + "SOUTH WEST" + postfix_directional, r"\1S W\2"),
    (prefix_directional + "SOUTH EAST" + postfix_directional, r"\1S E\2")]
directional_abbr = list(map(compile_RE_tuple, directional_abbr_not_compiled))

prefix_POBox = r"(.*[\s]+|^|.*\()"
postfix_POBox = r"([\s]+|$)"
postfix_POBox_digits = r"([\d]+.*$)"
poBox_abbr_not_compiled = [
    (prefix_POBox + "POST OFFICE BOX" + postfix_POBox, r"\1PO BOX\2"),
    (prefix_POBox + "P O BOX" + postfix_POBox, r"\1PO BOX\2"),
    (prefix_POBox + "POBOX" + postfix_POBox, r"\1PO BOX\2"),
    (prefix_POBox + "POBOX" + postfix_POBox_digits, r"\1PO BOX \2"),
    (prefix_POBox + "PO BOX" + postfix_POBox_digits, r"\1PO BOX \2")
]
poBox_abbr = list(map(compile_RE_tuple, poBox_abbr_not_compiled))

prefix_name = r"(.*)"
name_abbr_not_compiled = [
    (prefix_name + r" LLC", r"\1"),
    (prefix_name + r" INC", r"\1")]
name_abbr = list(map(compile_RE_tuple, name_abbr_not_compiled))


def hash_string(value):
    """Takes a string and creates a hash string from it

    Keyword arguments:
    value -- string to encode
    """
    return hashlib.sha3_512(value.encode()).hexdigest()


def create_hash_column(df_data, columns, hash_name="hash_id"):
    """ takes a dataframe and a column list in order to create a hash column.

    Keyword arguments:
    df_data -- dataframe to deduplicate
    columns -- columns to use in hash function
    hash_name -- an optional name for the new hash column, defaults to hash_id
    """
    df_data[hash_name] = df_data[columns].apply(
        lambda row: hash_string(
            '_'.join(row.values.astype(str))), axis=1).astype(StringDtype())
    return df_data


def clean_address_data(value):
    """ Function meant for cleaning up address data.

    Keyword arguments:
    value -- address string to clean up
    """
    value = str(value).strip()  # remove beginning and ending white space
    value = re.sub(r'\s+', ' ', value)  # remove extra spaces
    # Shorten common road values
    for pattern_tuple in road_abbr:
        value = re.sub(pattern_tuple[0], pattern_tuple[1], value)
    # Shorten common directional values
    for pattern_tuple in directional_abbr:
        value = re.sub(pattern_tuple[0], pattern_tuple[1], value)
    # Shorten common location values
    for pattern_tuple in location_abbr:
        value = re.sub(pattern_tuple[0], pattern_tuple[1], value)
    # Clean up PO Boxes
    for pattern_tuple in poBox_abbr:
        value = re.sub(pattern_tuple[0], pattern_tuple[1], value)
    return value


def clean_name_data(value):
    """ Function meant for cleaning up name data.

    Keyword arguments:
    value -- name string to clean up
    """
    value = re.sub(r'\\', ' ', value)
    value = str(value).strip()  # remove beginning and ending white space
    value = re.sub(r'\s+', ' ', value)  # remove extra spaces
    for pattern_tuple in name_abbr:
        value = re.sub(pattern_tuple[0], pattern_tuple[1], value)
    return value


def remove_fuzzy_matches(
        df_data, groupby_vals=[], match_field="", fuzzy_score=80):
    def check_fuzzy(df):
        dupl_indexes = []
        for i in range(len(df.values) - 1):
            for j in range(i + 1, len(df.values)):
                if fuzz.token_sort_ratio(df.values[i],
                                         df.values[j]) >= fuzzy_score:
                    dupl_indexes.append(df.index[j])
        return dupl_indexes
    df_new = df_data.copy()
    indexes = df_new.groupby(
        groupby_vals)[match_field].apply(check_fuzzy)

    for index_list in indexes:
        df_new.drop(index_list, inplace=True)

    return df_new


def multi_processing(df_data, apply_function, groupby=""):
    cpus = multiprocessing.cpu_count()

    if groupby:
        grouped = df_data.groupby(groupby)
        df1_splits = [grouped.get_group(group) for group in grouped.groups]
    else:
        df1_splits = np.array_split(df_data, cpus)

    with Pool(processes=cpus) as pool:
        results = pool.map(apply_function, df1_splits)
        df_result = pandas.concat(results)

    return df_result.reset_index(drop=True)
