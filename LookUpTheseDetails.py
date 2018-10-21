from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster('local').setAppName('LookupTheseDetails')
sc = SparkContext(conf = conf)

def parseData(line):
    fields = line.split()
    userId = int(fields[0])
    movieId = int(fields[1])
    rating = int(fields[2])
    time = fields[3]
    return (movieId, (userId, rating, time)) #Table Fields

def parseMovie(line):
    fields = line.split('|')
    movieId = int(fields[0])
    movieName = fields[1]
    # imdb = fields[3]
    return (movieId, movieName) #Table Fields

dataRdd = sc.textFile('file:///sparkCourse/ml-100k/u.data')
dataRdd_parsed = dataRdd.map(parseData)
movieRdd = sc.textFile('file:///sparkCourse/ml-100k/u.item')
movieRdd_parsed = movieRdd.map(parseMovie)
#
# movieid = movieRdd_parsed.max()
# print(movieid)
# lookup = dataRdd_parsed.lookup(movieid[0])
# print(lookup)

keys = [] #often used to lookup some values based on the large value set, think of it as like VLOOKUP.
#But if the point is to join the entire dataset then there is better operaters to use like join.
results = movieRdd_parsed.collect()
for result in results:
    keys.append(result[0])

# for key in keys:
#     lookup = dataRdd_parsed.lookup(key)
#     print(lookup)
