from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import combinations
from pyspark.sql import Window
import sys

class Project2:
    def __init__(self):
        self.invoiceNo = "invoiceNo"        # the unique ID to record one purchase transaction
        self.description = "description"    # the name of the item in a transaction
        self.quantity = "quantity"          # the amount of the items purchased
        self.invoiceDate = "invoiceDate"    # the time of the transaction
        self.unitPrice = "unitPrice"        # the price of a single item

    def run(self, inputPath, outputPath, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Fill in your code here
        
        # define schema of dataframe
        schema = StructType([\
                    StructField(self.invoiceNo, IntegerType(), True),\
                    StructField(self.description, StringType(), True),\
                    StructField(self.quantity, IntegerType(), True),\
                    StructField(self.invoiceDate, StringType(), True),\
                    StructField(self.unitPrice, FloatType(), True)])
        
        # get data from input file
        fileDF = spark.read.format("csv").option("header", "false").schema(schema).load(inputPath)
        dataDF = fileDF.select(self.invoiceNo, self.description, self.invoiceDate)

        # format invoiceDate
        dataDF = dataDF.withColumn(self.invoiceDate, to_timestamp(self.invoiceDate, "dd/M/yyyy h:mm:ss a"))
        dataDF = dataDF.withColumn(self.invoiceDate, date_format(self.invoiceDate, "M/yyyy"))
        
        # get the list of itemSets and count monthlyInvoices for each (invoiceDate, invoiceNo) pairs
        monthlyInvoicesDF = dataDF.groupBy(self.invoiceDate, self.invoiceNo).agg(collect_set(self.description).alias("itemSets"))
        monthlyInvoicesDF = monthlyInvoicesDF.groupBy(self.invoiceDate).agg(count(self.invoiceNo).alias("monthlyInvoices"), collect_set("itemSets").alias("itemSets"))
        monthlyInvoicesDF = monthlyInvoicesDF.withColumn("itemSets", explode("itemSets"))

        # get itemSets containing 3 items
        itemSetsUdf = udf(lambda i : list(combinations(i, 3)), ArrayType(ArrayType(StringType())) )
        itemSetsDF = monthlyInvoicesDF.withColumn("itemSets", explode(itemSetsUdf("itemSets")))
        
        # sort description in itemSets and format itemSets to match output
        sortItemSetsUdf = udf(lambda i : f"({'|'.join(sorted(i))})")
        itemSetsDF = itemSetsDF.withColumn("itemSets", sortItemSetsUdf("itemSets"))

        # calculate support value
        monthlyItemSetsDF = itemSetsDF.groupBy(self.invoiceDate, "itemSets", "monthlyInvoices").agg(count("itemSets").alias("monthlyItemSets"))
        supportDF = monthlyItemSetsDF.withColumn("support", col("monthlyItemSets")/col("monthlyInvoices"))
        supportDF = supportDF.select(self.invoiceDate, "itemSets", "support")

        # get topK and sort output
        window = Window.orderBy(desc("support"), "itemSets").partitionBy(self.invoiceDate)
        topKDF = supportDF.withColumn("topK", row_number().over(window))
        topKDF = topKDF.filter(col("topK") <= int(k)).select(self.invoiceDate, "itemSets", "support")

        # format date and sort
        topKDF = topKDF.withColumn(self.invoiceDate, to_timestamp(self.invoiceDate, "M/yyyy")).orderBy(self.invoiceDate)
        topKDF = topKDF.withColumn(self.invoiceDate, date_format(self.invoiceDate, "M/yyyy"))
        
        # output to file
        topKDF.coalesce(1)
        topKDF.write.csv(outputPath)
        
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3])

