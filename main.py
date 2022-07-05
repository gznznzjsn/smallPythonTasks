import os

from dotenv import load_dotenv
import findspark
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, sum, when

findspark.init()
load_dotenv()

jar_path = os.getenv('JAR_PATH')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
host = os.getenv('HOST')
database = os.getenv('DATABASE')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

dFReader = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{host}/{database}") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", 'org.postgresql.Driver')

actor = dFReader.option("dbtable", "actor").load()
category = dFReader.option("dbtable", "category").load()
film_category = dFReader.option("dbtable", "film_category").load()
film_actor = dFReader.option("dbtable", "film_actor").load()
film = dFReader.option("dbtable", "film").load()
inventory = dFReader.option("dbtable", "inventory").load()
rental = dFReader.option("dbtable", "rental").load()
payment = dFReader.option("dbtable", "payment").load()
customer = dFReader.option("dbtable", "customer").load()
address = dFReader.option("dbtable", "address").load()
city = dFReader.option("dbtable", "city").load()

# 1
category.join(film_category, on="category_id") \
    .withColumn("amount", count("film_id").over(Window.partitionBy("category_id"))) \
    .select(col("name"), col("amount")) \
    .distinct() \
    .orderBy(col("amount").desc()) \
    .show()

# 2
actor.join(film_actor, on="actor_id").join(film, on="film_id") \
    .withColumn("total_rental_duration", sum("rental_duration").over(Window.partitionBy("first_name", "last_name"))) \
    .select(col("first_name"), col("last_name"), col("total_rental_duration")) \
    .distinct() \
    .orderBy(col("total_rental_duration").desc()) \
    .limit(10) \
    .show()

# 3
category.join(film_category, on="category_id") \
    .join(film, on="film_id") \
    .join(inventory, on="film_id") \
    .join(rental, on="inventory_id") \
    .join(payment, on="rental_id") \
    .withColumn("payment_amount", sum("amount").over(Window.partitionBy("name"))) \
    .select(col("name"), col("payment_amount")) \
    .orderBy(col("payment_amount").desc()) \
    .distinct() \
    .limit(1) \
    .show()

# 4
film.join(inventory, on="film_id", how="left") \
    .groupby("film_id", "title") \
    .agg(count("inventory_id").alias("number")) \
    .filter(col("number") == 0) \
    .select(col("title")) \
    .show()

# 5
tbl = category.join(film_category, on="category_id") \
    .join(film, on="film_id") \
    .join(film_actor, on="film_id") \
    .join(actor, on="actor_id") \
    .groupby("actor_id", "name", "first_name", "last_name") \
    .agg(count("film_id").alias("quantity")) \
    .filter(col("name") == "Children") \
    .select(col("first_name"), col("last_name"), col("quantity"))

tbl.filter(col("quantity").isin(tbl
                                .select(col("quantity"))
                                .distinct()
                                .orderBy(col("quantity").desc())
                                .limit(3).rdd.flatMap(lambda x: x).collect())) \
    .select(col("first_name"), col("last_name"), col("quantity")) \
    .orderBy(col("quantity").desc()) \
    .show()

# 6
customer.join(address, on="address_id") \
    .join(city, on="city_id") \
    .groupby("city_id", "city") \
    .agg(sum("active").alias("active_quantity"),
         count(when(col("active") == 0, True)).alias("inactive_quantity")) \
    .select(col("city"), col("active_quantity"), col("inactive_quantity")) \
    .orderBy(col("inactive_quantity").desc()) \
    .show()

# 7
tbl = city.join(address, on="city_id") \
    .join(customer, on="address_id") \
    .join(rental, on="customer_id") \
    .join(inventory, on="inventory_id") \
    .join(film, on="film_id") \
    .join(film_category, on="film_id") \
    .join(category, on="category_id") \
    .filter(col("return_date").isNotNull()) \
    .select(col("name").alias("category_name"), col("return_date"), col("rental_date"), col("city"))

df1 = tbl.filter(col("city").like("A%") | col("city").like("a%")) \
    .withColumn("total_duration",
                sum(col("return_date") - col("rental_date")).over(Window.partitionBy("category_name"))) \
    .distinct() \
    .select(col("category_name"), col("total_duration")) \
    .orderBy(col("total_duration").desc()) \
    .limit(1)
df2 = tbl.filter(col("city").like("%-%")) \
    .withColumn("total_duration",
                sum(col("return_date") - col("rental_date")).over(Window.partitionBy("category_name"))) \
    .distinct() \
    .select(col("category_name"), col("total_duration")) \
    .orderBy(col("total_duration").desc()) \
    .limit(1)

df1.union(df2).show(truncate=False)
