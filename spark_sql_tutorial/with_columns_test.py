import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('test').getOrCreate()


# spark withColumn() funtion is used to rename. change the value, convert the datatype
# of an existing DataFrame column and also can be used to create a new column.

data = [(("James","","Smith"),"36636","M","3000"), \
      (("Michael","Rose",""),"40288","M","4000"), \
      (("Robert","","Williams"),"42114","M","4000"), \
      (("Maria","Anne","Jones"),"39192","F","4000"), \
      (("Jen","Mary","Brown"),"","F","-1") \
]

schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
          StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', StringType(), True)
         ])

#df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

df2 = df.withColumn("salary", col("salary").cast("Integer"))
df2.show()
df2.printSchema()

df.printSchema()
df.withColumn("salary", col("salary")*100)
df.show()

df3 = df.withColumn("CopiedColumn", col("salary")*-1)
df3.show()

df.withColumnRenamed("gender", "sex").show(truncate=False)

df3.drop("CopiedColumn").show(truncate=False) 
