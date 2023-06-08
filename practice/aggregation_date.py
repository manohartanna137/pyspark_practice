from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType,IntegerType
from pyspark.sql.functions import expr,col,lit,udf,collect_list,approx_count_distinct,collect_set,first,last,sumDistinct
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


data1 = [("James", ["Sales","a"], 3000), \
    ("Michael", ["Sales","b"], 4600), \
    ("Robert",[ "Sales","c"], 4100), \
    ("Maria", ["Finance","d"], 3000), \
    ("James", ["Sales","e"], 3000), \
    ("Scott", ["Finance","f"], 3300), \
    ("Jen", ["Finance","g"], 3900), \
    ("Jeff", ["Marketing","h"], 3000), \
    ("Kumar", ["Marketing","i"], 2000), \
    ("Saif", ["Sales","k"], 4100),("Manohar",["Sales","l"],9000) \
  ]
columns1= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data1, schema = columns1)
df.printSchema()
df.show(truncate=False)
df.select(df.salary.cast("int"))
#Distinct
distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)

#Drop duplicates
df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)

#Drop duplicates on selected columns
dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)
print("filter")


df3=df.filter(df["salary"]>3500)
df3.show()

df4=df.dropDuplicates(["salary"])
df4.show()


df5=df.withColumn('salary',col("salary")*2)
df5.show()



df6 = df.withColumn("JOB2",col("department")[1])
df6.show()

data3 = [(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
      (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
      (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
      (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

schema3 = StructType([
        StructField('name', StructType([
            StructField('fname', StringType(), True),
            StructField('lname', StringType(), True)])),
        StructField('languages', ArrayType(StringType()),True),
        StructField('properties', MapType(StringType(),StringType()),True)
     ])
df_=spark.createDataFrame(data3,schema3)
df_.printSchema()
df_.show()

df_.select(df_.properties.getField("hair")).show()

# with column

df_.select(df_.languages[0]).show()
d=df_.withColumn("Country",lit("USA"))
d.show()


df2 = spark.createDataFrame([[6, 7, 3]], ["col1", "col2", "col3"])
df1 = spark.createDataFrame([[5, 2, 6]], ["col0", "col1", "col2"])

# Using allowMissingColumns
df3 = df1.union(df2)
df3.printSchema()
df3.show()


columns2 = ["Seqno","Name"]
data2 = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df9 = spark.createDataFrame(data=data2,schema=columns2)


df9.show(truncate=False)
df10=df9.select(df9.Seqno.cast("int"))
df10.printSchema()

data_sal = [("manohar",20000,1500),("vamsi",25000,1520),("jaggu",30000,1600)]
schema_sal=["name","salary","bonus"]
df_sal = spark.createDataFrame(data=data_sal,schema=schema_sal)
df_sal.show()

def addition(m,n):
    return m + n
# udf techniques
total_sal=udf(lambda m,n :addition(m,n),IntegerType() )

df_combine = df_sal.withColumn("Combined_sal",total_sal(df_sal.salary,df_sal.bonus))
df_combine.show()
# transform techniques
df_transform= df_sal.transform(lambda df_sal: df_sal.withColumn("is_a", col("salary") >= 18000))
df_transform.show()

#aggregate functions
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema5 = ["employee_name", "department", "salary"]
df_agg = spark.createDataFrame(data=simpleData, schema = schema5)
df_agg.printSchema()
df_agg.show(truncate=False)


#list all the rows
df_agg.select(collect_list("salary")).show()
# eliminating duplicates
df_agg.select(collect_set("salary")).show()

#getting first element in column
df_agg.select(first("salary")).show()

#getting last element in salary column
df_agg.select(last("salary")).show()

#getting max in salary column
df_agg.select(max("salary")).show()

# getting minimum salary
df_agg.select(min("salary")).show()

# sum of distinct salaries

df_agg.select(sumDistinct("salary")).show()

# date time functions
data_date=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df_date=spark.createDataFrame(data_date,["id","input"])
df_date.show()

df_date.select(current_date().alias("current_date")).show(1)

df_date.select(col("input"), date_format(col("input"), "MM-dd-yyyy").alias("date_format")).show()

df_date.select(col("input"),to_date(col("input"),"yyy-MM-dd")).show()

df_date.select(col("input"),datediff(current_date(),col("input")).alias("diiference")).show()

df_date.select(col("input"),months_between(current_date(),col("input")).alias("monthbetween")).show()









