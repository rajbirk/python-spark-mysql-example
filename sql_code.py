# required import modules
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# creating a configuration for context. here "Spark-SQL" is the name of the application and we will create local spark context.
conf = SparkConf().setAppName("Spark-SQL").setMaster("local")

# create a spark context. It is the entry point into all relational functionality in Spark. 
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# user defined schema for json file.
schema = StructType([StructField("name", StringType()), StructField("age", IntegerType()), StructField("address", StringType())])

# loading the contents of the json to the data frame with the user defined schema for json data.
df1 = sqlContext.jsonFile("people.json", schema)

# display the contents of the dataframe.
df1.show()

# display the schema of the dataframe.
df1.printSchema()

# to register dataframe as sqlContext table.
#sqlContext.registerDataFrameAsTable(df1, "person1")

# loading the contents from the MySql database table to the dataframe.
df = sqlContext.load(source="jdbc", url="jdbc:mysql://localhost:3306/test?user=root&password=password", dbtable="people")

# display the schema of the dataframe.
df.show()

# display the schema of the dataframe.
df.printSchema()

# data frame is euivalent to the relational table in spark SQL. To select the column from dataframe.
print df.select(df.name).collect()

# registering data frame as a temporary table. 
#df.registerTempTable("person")

# register dataframe as a sqlContext table.
sqlContext.registerDataFrameAsTable(df, "person")

# perform sql select query upon the registered context table person.
d = sqlContext.sql("select name from person")

# craeting new RDD by applying function to each row.
names = d.map(lambda p: "name: " + p.name)

# to print new row data.
for name in names.collect():
  print name
 
# to print list of tables in the current database. 
print sqlContext.tableNames()

# creating new data frame containing union of rows in two dataframes.
ndf = df.unionAll(df1)

# display contents of new dataframe.
ndf.show()

# to update data to mysql table. if you set overwrite to true it will truncate the table before performing insert.
df1.insertIntoJDBC(url="jdbc:mysql://localhost:3306/test?user=root&password=password", dbtable="person", overwrite=False)

# to create new table in database.if you set alloExisting to true, it will drop any table with the given name.
#df1.createJDBCTable(url="jdbc:mysql://localhost:3306/test?user=root&password=password", dbtable="person", allowExisting=False)
