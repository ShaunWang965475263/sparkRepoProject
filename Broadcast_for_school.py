# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster('local').setAppName('Broadcast_for_Schools')
# sc = SparkContext(conf = conf)
#


# results = rdd.collect()
# for i in range(1,100):
#     print(results[i])

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Broadcast_schools')
sc = SparkContext(conf = conf)

def func():
    zipDict = {}

    with open('pySpark_github/Registered_Business_Locations_-_San_Francisco.csv', encoding='utf8', errors='ignore') as f:
        codeList = []
        for line in f:
            fields = line.split(',')
            zipCode =fields[7]

            if zipCode not in codeList:
                codeList.append(zipCode)
                zipDict[zipCode] = []
                bus_type = fields[17]
                zipDict[zipCode].append(bus_type)
            else:
                bus_type = fields[17]
                if bus_type not in zipDict[zipCode]:
                    zipDict[zipCode].append(bus_type)
        return zipDict


NameDict = sc.broadcast(func())

def parseLine(line):
    fields = line.split(',')
    school = fields[0]
    # print(school)
    if '94' not in fields[5]:
        category = fields[6]
        zipCode = fields[11]
    else:
        category = fields[5]
        zipCode = fields[12]

    return (zipCode, (school, category))


rdd = sc.textFile('file:///sparkCourse/pySpark_github/Schools.csv').map(parseLine)
rdd1 = rdd.reduceByKey(lambda x,y: (x[0]+','+y[0], x[1]+','+y[1]))


rdd1.map(lambda x: NameDict.value[x[0]])


results = rdd1.collect()
for i in range(1,2):
    print(results[i])
    print(NameDict.value[results[i][0]])
    # print(results[i])

# sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))
