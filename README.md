Part A: Basics (1–50)

Data Setup Example (for all questions in Part A):

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PySparkInterview").getOrCreate()

data = [
    (1,"Alice",29,"NY"),
    (2,"Bob",31,"CA"),
    (3,"Cathy",25,"TX"),
    (4,"David",35,"NY"),
    (5,"Eve",28,"CA")
]

columns = ["id","name","age","state"]
df = spark.createDataFrame(data, columns)
df.show()


Questions 1–50 with solutions (explicit, no skipping):

Select a single column

df.select("name").show()


Select multiple columns

df.select("name","age").show()


Filter rows where age > 30

df.filter(df.age > 30).show()


Filter rows where state = 'CA' and age > 28

df.filter((df.state=="CA") & (df.age>28)).show()


Add a new column with age + 5

df.withColumn("age_plus_5", col("age")+5).show()


Drop a column

df.drop("state").show()


Rename a column

df.withColumnRenamed("name","full_name").show()


Count total rows

df.count()


Count distinct states

df.select("state").distinct().count()


Sort by age ascending

df.orderBy(col("age").asc()).show()


Sort by age descending

df.orderBy(col("age").desc()).show()


Limit to top 3 rows

df.limit(3).show()


Take first row

df.first()


Take first 2 rows

df.take(2)


Collect all rows as list

df.collect()


Show schema

df.printSchema()


Describe DataFrame

df.describe().show()


Filter nulls in a column (simulate with nulls)

from pyspark.sql import Row
df_with_null = spark.createDataFrame([Row(id=1,name="Alice"), Row(id=2,name=None)])
df_with_null.filter(df_with_null.name.isNotNull()).show()


Filter rows where column is null

df_with_null.filter(df_with_null.name.isNull()).show()


Drop duplicate rows

df_dup = df.union(df)  # create duplicates
df_dup.dropDuplicates().show()


Replace values in a column (withColumn + when)

from pyspark.sql.functions import when
df.withColumn("state_new", when(col("state")=="NY","New York").otherwise(col("state"))).show()


Create new column based on condition

df.withColumn("is_adult", when(col("age")>=30, True).otherwise(False)).show()


RDD: Convert DataFrame to RDD

rdd = df.rdd
rdd.collect()


RDD: Map function to increase age by 1

rdd.map(lambda x: (x.id,x.name,x.age+1,x.state)).collect()


RDD: Filter age > 30

rdd.filter(lambda x: x.age>30).collect()


RDD: Reduce to sum of ages

from functools import reduce
reduce(lambda a,b: a+b, rdd.map(lambda x: x.age).collect())


RDD: Count by key (state)

rdd.map(lambda x: (x.state,1)).reduceByKey(lambda a,b: a+b).collect()


RDD: Distinct states

rdd.map(lambda x: x.state).distinct().collect()


RDD: Take top 2 oldest

rdd.takeOrdered(2,key=lambda x: -x.age)


RDD: Union two RDDs

rdd2 = spark.createDataFrame([(6,"Frank",32,"TX")], columns).rdd
rdd.union(rdd2).collect()


RDD: Intersection

rdd.intersection(rdd2).collect()


RDD: Subtract

rdd.subtract(rdd2).collect()


RDD: Sort by age

rdd.sortBy(lambda x: x.age, ascending=False).collect()


RDD: MapValues (for key-value RDD)

kv_rdd = rdd.map(lambda x: (x.name,x.age))
kv_rdd.mapValues(lambda x: x+1).collect()


RDD: Filter by value in key-value

kv_rdd.filter(lambda x: x[1]>30).collect()


DataFrame: Convert column to string

df.withColumn("age_str", col("age").cast("string")).show()


DataFrame: Limit, select, and order combined

df.orderBy(col("age").desc()).select("name","age").limit(3).show()


DataFrame: Add multiple columns

df.withColumn("age_plus_10", col("age")+10).withColumn("age_times_2", col("age")*2).show()


Filter using isin

df.filter(col("state").isin("NY","CA")).show()


Filter using like

df.filter(col("name").like("A%")).show()


Filter using rlike (regex)

df.filter(col("name").rlike("^A.*e$")).show()


Filter using between

df.filter(col("age").between(28,32)).show()


Count per state (groupBy)

df.groupBy("state").count().show()


Sum ages per state

from pyspark.sql.functions import sum
df.groupBy("state").agg(sum("age")).show()


Avg age per state

from pyspark.sql.functions import avg
df.groupBy("state").agg(avg("age")).show()


Max age per state

from pyspark.sql.functions import max
df.groupBy("state").agg(max("age")).show()


Min age per state

from pyspark.sql.functions import min
df.groupBy("state").agg(min("age")).show()


Count distinct names

from pyspark.sql.functions import countDistinct
df.agg(countDistinct("name")).show()


RDD: Map and reduce to sum ages > 30

rdd.filter(lambda x: x.age>30).map(lambda x: x.age).reduce(lambda a,b:a+b)


RDD: Map to tuple (state, age), groupByKey, sum per state

rdd.map(lambda x: (x.state,x.age)).groupByKey().map(lambda x: (x[0],sum(x[1]))).collect()
