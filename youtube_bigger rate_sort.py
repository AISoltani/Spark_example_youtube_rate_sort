from pyspark import SparkContext, SparkConf
from collections import namedtuple

conf = SparkConf().setMaster("local").setAppName("AI-Soltani")
sc = SparkContext(conf=conf)
Record = namedtuple("Record",
                    ["videoID", "uploader", "age", "category", "length", "views", "rate", "ratings", "comments",
                     "relatedIDs"])

def parse_record(s):
    try:
        fields = s.split('\t')
        return Record(fields[0], fields[1], int(fields[2]), fields[3], int(fields[4]), int(fields[5]), float(fields[6]),
                      int(fields[7]), int(fields[8]), fields[9])
    except:
        pass

data = sc.textFile("youtube.txt")
data = data.map(parse_record).filter(lambda x: x != None).cache()
cat_list=['Comedy','Sports','Film & Animation','Music','Entertainment']
for i in cat_list:
  cat_filter = data.filter(lambda x: x.category == i).cache()
  rate_bigger_4 = cat_filter.filter(lambda x: x.rate >= 4).cache()
  rate_less_4 = cat_filter.filter(lambda x: x.rate < 4).cache()
  mean_bigger_4 = rate_bigger_4.map(lambda x: x.views).sum() / rate_bigger_4.count()
  mean_less_4 = rate_less_4.map(lambda x: x.views).sum() / rate_less_4.count()
  print("View average bigger than 4 of ",i,"is: ", mean_bigger_4)
  print("View average less than 4 of ",i,"is: ", mean_less_4)
