import sys
from pyspark import SparkContext

def extractCplaints(partId, records):
    if partId == 0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        #filter out invalid data 
        if len(row)>7 and row[0][:4].isdigit():
            yield ((row[0][:4], row[1].lower(), row[7].lower()),1)

def main(sc):
    sys.argv[1] = 'complaints_small.csv'
    COMPLAINTS = sc.textFile(sys.argv[1], use_unicode=True).cache()
    complaints = COMPLAINTS.mapPartitionsWithIndex(extractComplaints)
    output1 = complaints.groupByKey().mapValues(lambda values: sum(values))
    output2 = output1.map(lambda x: ((x[0][1],x[0][0]),x[1]))\
        .groupByKey()\
        .mapValues(lambda x: (sum(x), len(x), int(max(x)*100.0/len(x)+0.5)))
    output3 = output2.map(lambda x: [x[0][0],x[0][1],x[1][0],x[1][1],x[1][2]])
    output3.saveAsTextFile("complaints_out")

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)