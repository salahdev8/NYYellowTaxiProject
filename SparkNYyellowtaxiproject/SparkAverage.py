import sys
from pyspark import SparkContext
from pyspark import SparkConf
import datetime

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: SparkAverage <file>"
        exit(-1)


    def pickupAverage(pickup, dropoff, amount):

        pickupArray = pickup.split(" ")
        dropoffArray = dropoff.split(" ")

        pickupdate = datetime.datetime.strptime(pickup, "%Y-%m-%d %H:%M:%S")
        dropoffdate = datetime.datetime.strptime(dropoff, "%Y-%m-%d %H:%M:%S")

        a = datetime.datetime(pickupdate.year, pickupdate.month, pickupdate.day, pickupdate.hour, pickupdate.minute,
                              pickupdate.second)
        b = datetime.datetime(dropoffdate.year, dropoffdate.month, dropoffdate.day, dropoffdate.hour,
                              dropoffdate.minute, dropoffdate.second)

        d = b - a

        duration = float(d.seconds) / 60
        amountint = float(amount)
        if duration !=0:
            averagearning = amountint / duration
        else:
            averagearning = 1



        return averagearning


    def pickupmoment(pickup):

        pickupdate = datetime.datetime.strptime(pickup, "%Y-%m-%d %H:%M:%S")
        pickuphour = pickupdate.hour
        pickuphourplus = pickupdate.hour + 1
        result = ""
        result += pickupdate.strftime("%A") + "[" + str(pickuphour) + ":" + str(pickuphourplus) + "]"
        return result

    def toCSVLine(data):
        return ','.join(str(d) for d in data)


    sc = SparkContext()
    logfile = sys.argv[1]
    #tags = sc.textFile(logfile)
    #tagsheader = tags.first()
    #header = sc.parallelize([tagsheader])
    #tagsdata = tags.subtract(header)


    #data = sc.textFile(logfile)
    #header = sc.parallelize([data.first()])

    #header = data.take(1)[0]
    #rows = data.filter(lambda line: line != header)

    first = sc.textFile(logfile).map(lambda line: line.split(',')) \
        .map(lambda fields: (pickupmoment(fields[1]), pickupAverage(fields[1], fields[2], fields[16]))) \
        .mapValues(lambda value: (value, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda res: res[0] / res[1])

    for ele in first.collect():
        out = str(ele)
        out1 = out.replace('(', '')
        out2 = out1.replace('\'', '')
        outfinal = out2.replace(')', '')
        print outfinal



    sc.stop()