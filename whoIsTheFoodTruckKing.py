from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('whoIsTheKing')
sc = SparkContext(conf = conf)



def parseLine(line):
    fields = line.split(',')
    # print(fields)
    locationID =fields[0]
    applicant = fields[1]
    return (locationID, applicant)


rdd = sc.textFile('file:///sparkCourse/pySpark_github/Mobile_Food_Facility_Permit.csv')

mapped = rdd.map(parseLine)

reduced = mapped.reduceByKey(lambda x, y: x+y)

results = reduced.collect()
for i in range(1,10):
    print(results[i])
