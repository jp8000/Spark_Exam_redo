from pyspark.sql.types import StringType, ArrayType, StructType, StructField, LongType, DoubleType

# Defining the structure and data types of a DataFrame, it specifies the column names, their data types. 
# Null=True: Fields can be left empty or have missing values  
# The schrma will help making the code more type safe
Schema_columns = StructType([
    StructField("customerId", StringType(), nullable=True),
    StructField("forename", StringType(), nullable=True),
    StructField("surname", StringType(), nullable=True),
    StructField("accounts", ArrayType(StringType()), nullable=True),
    StructField("numberAccounts", LongType(), nullable=True),
    StructField("totalBalance", LongType(), nullable=True),
    StructField("averageBalance", DoubleType(), nullable=True),
    StructField("address", StructType([
        StructField("streetNumber", LongType(), nullable=True),
        StructField("streetName", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True)
    ]))
])


