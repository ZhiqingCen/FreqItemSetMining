from pyspark import SparkContext, SparkConf
from itertools import combinations
from operator import add
from datetime import datetime
import heapq
import sys

class Project2:
    def __init__(self):
        self.invoiceNo = 0      # the unique ID to record one purchase transaction
        self.description = 1    # the name of the item in a transaction
        self.quantity = 2       # the amount of the items purchased
        self.invoiceDate = 3    # the time of the transaction
        self.unitPrice = 4      # the price of a single item

    def run(self, inputPath, outputPath, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        # Fill in your code here
        # detect the top-k frequent item sets from the log for each month
        
        # load data from file
        file = sc.textFile(inputPath)
        lines = file.map(lambda line : line.split(","))
        
        # keep only invoiceNo, description and formatted invoiceDate data
        data = lines.map(lambda i : (i[self.invoiceNo], (set(i[self.description]), i[self.invoiceDate].split(" ", 1)[0].split("/", 1)[1])))

        # group items according to each invoiceNo
        invoices = data.reduceByKey(lambda i, j : (i[self.invoiceNo] | j[0], i[1]))
        invoices = invoices.map(lambda i : (i[self.invoiceNo], (sorted(i[1][0]), i[1][1])))

        # get item sets containing 3 items
        threeItems = invoices.map(lambda i : (i[0], (list(combinations(i[1][0], 3)), i[1][1])))

        # map according to item sets
        itemSets = threeItems.flatMap(lambda i : [(i[0], ("|".join(items), i[1][1])) for items in i[1][0]])
        invoicesByNo = itemSets.map(lambda i : ((i[0], i[1][1]), 1)).groupByKey().mapValues(len)
        
        # count number of unique invoices for each month
        monthlyInvoices = invoices.map(lambda i : (i[0], i[1][1])).map(lambda i : (i[1], 1)).groupByKey().mapValues(len)
        
        # count number of unique itemSets for each month
        monthlyItemSets = itemSets.map(lambda i : ((i[1][1], i[1][0]), 1)).groupByKey().mapValues(len).map(lambda i : (i[0][0], (i[0][1], i[1])))

        # calculate support value
        # convert to output format = MONTH/YEAR,(Item1|Item2|Item3), support value
        support = monthlyItemSets.join(monthlyInvoices).map(lambda i : (i[0], f"({i[1][0][0]})", i[1][0][1] / i[1][1]))
        
        # sort output and get topK rows only
        monthlySortSupport = support.groupBy(lambda i : i[0])
        topK = monthlySortSupport.mapValues(lambda i : heapq.nsmallest(int(k), i, key=lambda j : (datetime.strptime(j[0], "%m/%Y"), -j[2], j[1])))
        topK = topK.flatMap(lambda i : i[1]).map(lambda i : (i[0], i[1], str(i[2])))#.map(lambda i : ",".join(i)).coalesce(1)
        topK = topK.sortBy(lambda i : datetime.strptime(i[0], "%m/%Y")).map(lambda i : ",".join(i)).coalesce(1)
        
        # output to file
        topK.saveAsTextFile(outputPath)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3])
