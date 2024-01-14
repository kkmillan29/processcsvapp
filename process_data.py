import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf
from pyspark.sql.types import StringType


def process_data(spark, input_path):
    # Read CSV file into DataFrame
    raw_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Variable to keep track of invalid names count
    invalid_name_count = spark.sparkContext.accumulator(0)

    # Variable to keep track of AirlineCodeBulk with no value count
    empty_airline_code_count = spark.sparkContext.accumulator(0)

    # Define a UDF to extract Title, FirstName, MiddleName, and LastName
    def extract_name_info(input_string, field):
        nonlocal invalid_name_count

        title_pattern = r'\b(MR|MRS|MS|MISS|MASTER|DR|PROF)\b'

        title, firstname, middlename, lastname = None, None, None, None

        matches = re.findall(title_pattern, input_string, re.IGNORECASE)
        if matches:
            title = matches[0]

        if "/" in input_string:
            index_of_slash = input_string.index("/")
            before_slash = input_string[:index_of_slash]
            if len(before_slash) != 0:
                lastname = re.sub(title_pattern, '', before_slash, flags=re.IGNORECASE).strip()

            after_slash = input_string[index_of_slash + 1:]
            if len(after_slash) != 0:
                afterstr = re.sub(title_pattern, '', after_slash, flags=re.IGNORECASE).strip()
                after_list_str = afterstr.split(" ")
                if len(after_list_str) > 0:
                    firstname = after_list_str[0]
                    if len(after_list_str) > 1:
                        middlename = ' '.join(after_list_str[1:])

        elif " " in input_string:
            input_string = re.sub(title_pattern, '', input_string, flags=re.IGNORECASE).strip()
            list_str = input_string.split(" ")
            if len(list_str) >= 3:
                firstname = list_str[0]
                lastname = list_str[len(list_str) - 1]
                middlename = ' '.join(list_str[1:len(list_str) - 2])
            elif len(list_str) == 2:
                firstname = list_str[0]
                lastname = list_str[1]
            elif len(list_str) == 1:
                firstname = list_str[0]

        # Check for invalid names
        if title is None and firstname is None and middlename is None and lastname is None:
            invalid_name_count += 1

        if field == 'title':
            return title
        elif field == 'firstname':
            return firstname
        elif field == 'middlename':
            return middlename
        elif field == 'lastname':
            return lastname
        else:
            return None

    # Define UDFs for each field
    extract_title_udf = udf(lambda x: extract_name_info(x, 'title'), StringType())
    extract_firstname_udf = udf(lambda x: extract_name_info(x, 'firstname'), StringType())
    extract_middlename_udf = udf(lambda x: extract_name_info(x, 'middlename'), StringType())
    extract_lastname_udf = udf(lambda x: extract_name_info(x, 'lastname'), StringType())

    # Apply the UDFs to create new columns
    raw_df = raw_df.withColumn("Title", extract_title_udf(col("PassengerName")))
    raw_df = raw_df.withColumn("FirstName", extract_firstname_udf(col("PassengerName")))
    raw_df = raw_df.withColumn("MiddleName", extract_middlename_udf(col("PassengerName")))
    raw_df = raw_df.withColumn("LastName", extract_lastname_udf(col("PassengerName")))

    # Splitting AirlineCodeBulk dynamically
    max_codes = raw_df.selectExpr("max(size(split(AirlineCodeBulk, '-'))) as max_codes").first().max_codes
    for i in range(1, max_codes + 1):
        raw_df = raw_df.withColumn(f'AirlineCode{i}', split(raw_df['AirlineCodeBulk'], '-').getItem(i - 1))

    # Check for empty AirlineCodeBulk and increment count
    empty_airline_code_count += raw_df.filter(col("AirlineCodeBulk").isNull()).count()

    # Return both raw_df, invalid_name_count, and empty_airline_code_count
    return raw_df, invalid_name_count.value, empty_airline_code_count.value
