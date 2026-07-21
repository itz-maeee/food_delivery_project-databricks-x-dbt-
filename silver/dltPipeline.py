import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name="stage_order"
)
def stage_order():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/delivery_bronze/bronzevolume/order/data")
    return df

@dlt.view(
    name = "trans_order"
)
def trans_order():
    df = spark.readStream.table("stage_order")
    df = df.withColumn("total_amount",col("total_amount").cast(DoubleType()))\
            .withColumn("modifiedDate", current_timestamp())\
            .withColumn("order_date",to_date(col("order_date")))\
            .drop("_rescued_data")
    return df

rules = {
    "rule1": "order_id IS NOT NULL",
    "rule2": "customer_id IS NOT NULL"
}

@dlt.table(
  name="silver_order"
)
@dlt.expect_all_or_drop(rules)
def silver_order():
    df = spark.readStream.table("trans_order")
    return df


##############################################################################################
#Delivery Partners
@dlt.view(
    name = "trans_partners"
)
def trans_partners():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/delivery_bronze/bronzevolume/partners/data")
    df = df.withColumn("modifiedDate", current_timestamp())\
            .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_partners")

dlt.create_auto_cdc_flow(
    target = "silver_partners",
    source = "trans_partners",
    keys = ["partner_id"],
    sequence_by = "modifiedDate",
    stored_as_scd_type="1"
)


##############################################################################################
#Customers data

@dlt.view(
    name = "trans_customers"
)
def trans_customers():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/delivery_bronze/bronzevolume/customers/data")
    df = df.withColumn("modifiedDate", current_timestamp())\
            .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_customers")

dlt.create_auto_cdc_flow(
    target = "silver_customers",
    source = "trans_customers",
    keys = ["customer_id"],
    sequence_by = "modifiedDate",
    stored_as_scd_type="1"
)

##############################################################################################
#Restaurants data

@dlt.view(
    name = "trans_restaurants"
)
def trans_restaurants():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/delivery_bronze/bronzevolume/restaurants/data")
    df = df.withColumn("modifiedDate", current_timestamp())\
            .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_restaurants")

dlt.create_auto_cdc_flow(
    target = "silver_restaurants",
    source = "trans_restaurants",
    keys = ["restaurant_id"],
    sequence_by = "modifiedDate",
    stored_as_scd_type="1"
)



##############################################################################################
#Silver Business data
@dlt.table(
    name = "silver_business"
)

def silver_business():
    # Read the main order stream
    orders = dlt.readStream("silver_order")
    
    # Read dimension tables and drop duplicate columns
    customers = dlt.read("silver_customers")
    partners = dlt.read("silver_partners").drop("city")
    restaurants = dlt.read("silver_restaurants").drop("city")
    
    # Join all tables
    df = orders\
            .join(customers, ["customer_id"])\
            .join(partners, ["partner_id"])\
            .join(restaurants, ["restaurant_id"])\
            .drop("modifiedDate")
    return df



















