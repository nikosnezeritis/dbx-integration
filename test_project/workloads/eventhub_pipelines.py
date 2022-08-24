# Rename columns
MAPPING = {"ts": "eventhub_timestamp", "cst": "customer_id", "cid": "contact_id", "mid": "maersk_id"}


def parse_eventhub_timestamp(data: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    eventhub_timestamp = data.withColumn("eventhub_timestamp",
                                         (F.col("eventhub_timestamp") / 1000).cast(T.TimestampType()))
    return eventhub_timestamp


def apply_function_to_column(data: pyspark.sql.DataFrame, column_name: str, new_column_name: Optional[str] = None,
                             func) -> pyspark.sql.DataFrame:
    """
    Apply a function to the <column_name> column of the <data> dataframe.

    Arguments:
        data:            The dataframe, which has a column we wish to modify using a function
        column_name:     The name of the column we wish to modify
        new_column_name: The new name of the column, if we want to change it
        func:            The function we want to apply to the column

    Returns:
        pyspark.sql.DataFrame
    """
    data = data.withColumn(new_column_name, func(column_name))
    return data


def resample(data: pyspark.sql.DataFrame, granularity: str, column_name: str, new_column_name: Optional[str] = None,
             drop_original: bool = True) -> pyspark.sql.DataFrame:
    """
    Resample a dataframe to a given granularity.

    Arguments:
        data:            The dataframe that we wish to resample
        granularity:     The new granularity that we want to have, e.g. "hour"
        column_name:     The name of the column that has the datetime that we want to resample
        new_column_name: The new column name. If it's None, then the name doesn't change
        drop_original:   True if we want to drop the original column

    Returns:
        pyspark.sql.DataFrame
    """
    if new_column_name is None:
        new_column_name = column_name
    data = data.withColumn(new_column_name, F.date_trunc(granularity, column_name))
    if drop_original:
        data = data.drop(column_name)
    return data


def rename_columns(data: pyspark.sql.DataFrame, column_names_dict: dict) -> pyspark.sql.DataFrame:
    """
      Rename the columns of a pyspark datframe according to the mapping rules defined in a dictionary.

      Arguments:
          data:              The dataframe whose columns we wish to rename
          column_names_dict: A dictionary representing the mapping rule according to which the renaming of columns should
                             take place. Its keys are the original names and its values the new names

      Returns:
          a pyspark dataframe with renamed columns
      """
    renamed_data = data.select(
        [F.col(column_name).alias(column_names_dict.get(column_name, column_name)) for column_name in data.columns])
    return renamed_data


def filter_by_column(data: pyspark.sql.DataFrame, column_name: str = "action",
                     filter: str = "Flex Hub%") -> pyspark.sql.DataFrame:
    """
    Get the data portion, in which the given column includes a certain substring.

    Arguments:
        data:        The dataframe we want to filter
        column_name: The column we want to use to filter the dataframe
        filter:      The substring that we want the column to include

    Returns:
        The filtered dataframe.
    """
    data = data.filter(F.col(column_name).like(filter))
    return data


def set_types(data: pyspark.sql.DataFrame, column_target_type_mapping: Dict[str, T.DataType]) -> pyspark.sql.DataFrame:
    """
    Change the data types of some columns as defined in a dictionary.

    Arguments:
        data:                       The dataframe, whose types we want to modify.
        column_target_type_mapping: The dictionary that defines the new types, of the columns that we want to change.

    Returns:
        pyspark.sql.DataFrame
    """
    original_columns = set(data.columns)
    columns_that_change_type = set(list(column_target_type_mapping.keys()))
    columns_that_dont_change_type = list(original_columns.difference(columns_that_change_type))
    data_with_correct_types = data.select(*columns_that_dont_change_type,
                                          *(F.col(column_name).cast(target_type).alias(column_name) for
                                            column_name, target_type in column_target_type_mapping.items()))
    return data_with_correct_types


def parse_json(data: pyspark.sql.DataFrame, target_schema: T.StructType, source_schema: T.StructType,
               json_column_name: str, new_column_name: Optional[str] = None) -> pyspark.sql.DataFrame:
    """
    Parse the contents of a json-valued column, given its original schema into separate columns according to a target schema

    Arguments:
        data:             The dataframe whose json-valued column we want to parse
        target_schema:    The desired resulting schema
        source_schema:    The original schema
        json_column_name: The json-valued column name
        new_column_name:  The new name of the json valued column

    Returns:
      The original dataframe plus separate columns for each json key.
    """

    if new_column_name is None:
        new_column_name = json_column_name
    # Deserialise json and rename the respective column
    data = data.withColumn(new_column_name, F.from_json(F.col(json_column_name), target_schema))
    # Get separate columns from the json as well as the original columns
    data = data.select(f"{new_column_name}.*", *[field.name for field in source_schema])
    return data


def set_google_analytics_action(data: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Create the google_analytics_action column according to the following logic: If the original action column includes
    the "Show" substring, then the column should have the "Open Widget" value, else it should have the
    "Confirm Purchase" value.

    Arguments:
        data: The dataframe, which we want to create the google_analytics_action column on.

    Returns:
        pyspark.sql.DataFrame
    """
    data = data.withColumn("google_analytics_action",
                           F.when((data.action.like("%Show")), "Open Widget").otherwise("Confirm Purchase"))
    return data