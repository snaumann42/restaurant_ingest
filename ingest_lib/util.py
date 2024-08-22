import hashlib
import base64


def hash_string(value):
    """Takes a string and creates a hash string from it

    Keyword arguments:
    value -- string to encode
    """
    # import base64
    # base64.urlsafe_b64encode(
    return base64.urlsafe_b64encode(hashlib.sha3_512(value.encode()).digest())


def create_hash_column(df_data, columns, hash_name="hash_id"):
    """ takes a dataframe and a column list in order to create a hash column.

    Keyword arguments:
    df_data -- dataframe to deduplicate
    columns -- columns to use in hash function
    hash_name -- an optional name for the new hash column, defaults to hash_id
    """
    df_data[hash_name] = df_data[columns].apply(
        lambda row: hash_string('_'.join(row.values.fillna('').astype(str))),
                                axis=1)
    return df_data
