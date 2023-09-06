from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, collect_list 


from Schema import Schema_columns
from CustomerAccount import CustomerAccount, parse_address


# Create a Spark session
spark = SparkSession.builder.appName("spark").getOrCreate()


# Creating a list to store the customer account objects
customerAccounts = []


# Read account data from CSV - ACCOUNTS DATA
df_accounts_data = spark.read.option("header", "true").option("inferSchema", "false") \
    .csv("data/account_data.txt") 

# Read customer data from CSV - CUSTOMER DATA
df_customer_data = spark.read.option("header", "true").option("inferSchema", "false") \
    .csv("data/customer_data.txt") 

# Performing a join operation.  (accounts_data / customer_data)
Customer_Account_Output_Data = df_accounts_data.join(df_customer_data, "customerId")

Customer_Account_Output_Data.show()

# Add additional columns
# GroupBy: "customerId", "forename", "surname" as we need these columns for reference
Customer_Account_Output_Data_Added_Fields = Customer_Account_Output_Data.groupBy("customerId", "forename", "surname").agg(
    collect_list("accountId").alias("accounts"),
    count("accountId").alias("numberAccounts"),
    sum("balance").alias("totalBalance"),
    avg("balance").alias("averageBalance")
)
# print tables - QUESTION 1 TABLE
Customer_Account_Output_Data_Added_Fields.show()

# Save DataFrame as Parquet file
#Customer_Account_Output_Data_Added_Fields.write.parquet("Spark_Exam_Redo/data")


# Read the Parquet file from 
parquet_df = spark.read.parquet("data/output.parquet/part-00000-164e172d-36bd-4763-a1e2-f73c00e04c02-c000.snappy.parquet")

# Read address data from CSV - ADDRESS DATA
df_address_data = spark.read.option("header", "true").option("inferSchema", "false") \
    .csv("data/address_data.txt") 

# Performing a join between parquet_df and df_address_data using the customerId column
address_joined_parquet_df = parquet_df.join(df_address_data, "customerId")

# Prints to joined data
address_joined_parquet_df.show()


# Iterating over each row of the DataFrame 
for row in address_joined_parquet_df.collect():
    
    # extracting the values of specific columns
    customerId = row.customerId
    forename = row.forename
    surname = row.surname
    accounts = row.accounts
    address = row.address  


    # Calling the "parse_address" function to extract data from the column 'address'
    parsed_address = parse_address(address)

    # Creating CustomerAccount objects by iterating over the rows in the DataFrame, extracting the column values
    customer_account = CustomerAccount(
        customerId=customerId,
        forename=forename,
        surname=surname,
        accounts=accounts,
        streetNumber=parsed_address['streetNumber'],
        streetName=parsed_address['streetName'],
        city=parsed_address['city'],
        country=parsed_address['country']
    )

    # Appending/adding the CustomerAccount objects to the CustomeerAccouts list
    customerAccounts.append(customer_account)



# Creates a Spark DataFrame from the list "customerAccounts"
Customer_Account_Objects = spark.createDataFrame(customerAccounts)

# Defines the column order
column_order = [
    "customerId",
    "forename",
    "surname",
    "accounts",
    "streetNumber",
    "streetName",
    "city",
    "country"
    ]
                                
# Select columns in the desired order through "column_order"
Customer_Account_Parsed_Address = Customer_Account_Objects.select(*column_order)
                                

# Show the DataFrame.   truncate=False: displays data at full length.
Customer_Account_Parsed_Address.show(truncate=False)

# Prints all lines of data
# Customer_Account_Parsed_Address.show(Customer_Account_Parsed_Address.count(), truncate=False)

