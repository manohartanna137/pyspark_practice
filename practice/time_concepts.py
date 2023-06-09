from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp,from_unixtime,col,to_utc_timestamp,date_format,trim
from pyspark.sql.types import StringType,IntegerType,DateType,StructType,StructField

data=[("Washing Machine","1648770933000",20000,"Samsung","India","0001"),("Refrigerator ","1648770999000",35000," LG",None,"0002"),("Air Cooler","1648770948000",45000," Voltas",None,"0003")]
schema=StructType([StructField("Product Name",StringType(), nullable=True),
                   StructField("Issue Date",StringType(), nullable=True),
                   StructField("Price",IntegerType(), nullable=True),
                   StructField("Brand",StringType(), nullable=True),
                   StructField("Country",StringType(), nullable=True),
                   StructField("Product number",StringType(), nullable=True)])
spark=SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df=spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)




# conversion of date to timestamp format
df = df.withColumn("Issue Date", from_unixtime(col("Issue Date")/1000)) # converting to milli seconds
df = df.withColumn("Issue Date", date_format(col("Issue Date"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
df.show(truncate=False)

# conversion of timestamp to date type

df1=df.withColumn("date",date_format(col("Issue Date"),"yyyy-MM-dd"))
df1.show(truncate=False)

# removing extra spaces from column brand
df = df.withColumn("Brand", trim(col("Brand")))
df.show(truncate=False)

df=df.na.fill(" ",["Country"])
df.show(truncate=False)








