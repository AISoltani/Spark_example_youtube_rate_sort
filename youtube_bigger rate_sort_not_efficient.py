from pyspark import SparkContext, SparkConf
from collections import namedtuple

conf = SparkConf().setMaster("local").setAppName("Amir-Soltani")
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
data = data.map(parse_record)
data = data.filter(lambda x: x != None)
data.cache()


cat_filter1 = data.filter(lambda x: x.category == 'Comedy').cache()
rate_bigger_4 = cat_filter1.filter(lambda x: x.rate >= 4).cache()
rate_less_4 = cat_filter1.filter(lambda x: x.rate < 4).cache()
mean_bigger_4 = rate_bigger_4.map(lambda x: x.views).sum() / rate_bigger_4.count()
mean_less_4 = rate_less_4.map(lambda x: x.views).sum() / rate_less_4.count()
print("View average bigger than 4 of Comedy", mean_bigger_4)
print("View average less than 4 of Comedy", mean_less_4)

cat_filter2 = data.filter(lambda x: x.category == 'Sports').cache()
rate_bigger_42 = cat_filter2.filter(lambda x: x.rate >= 4).cache()
rate_less_42 = cat_filter2.filter(lambda x: x.rate < 4).cache()
mean_bigger_42 = rate_bigger_42.map(lambda x: x.views).sum() / rate_bigger_42.count()
mean_less_42 = rate_less_42.map(lambda x: x.views).sum() / rate_less_42.count()
print("View average bigger than 4 of Sports", mean_bigger_42)
print("View average less than 4 of Sports", mean_less_42)

cat_filter3 = data.filter(lambda x: x.category == 'Film & Animation').cache()
rate_bigger_43 = cat_filter3.filter(lambda x: x.rate >= 4).cache()
rate_less_43 = cat_filter3.filter(lambda x: x.rate < 4).cache()
mean_bigger_43 = rate_bigger_43.map(lambda x: x.views).sum() / rate_bigger_43.count()
mean_less_43 = rate_less_43.map(lambda x: x.views).sum() / rate_less_43.count()
print("View average bigger than 4 of Film & Animation", mean_bigger_43)
print("View average less than 4 of Film & Animation", mean_less_43)

cat_filter4 = data.filter(lambda x: x.category == 'Music').cache()
rate_bigger_44 = cat_filter4.filter(lambda x: x.rate >= 4).cache()
rate_less_44 = cat_filter4.filter(lambda x: x.rate < 4).cache()
mean_bigger_44 = rate_bigger_44.map(lambda x: x.views).sum() / rate_bigger_44.count()
mean_less_44 = rate_less_44.map(lambda x: x.views).sum() / rate_less_44.count()
print("View average bigger than 4 of Music", mean_bigger_44)
print("View average less than 4 of Music", mean_less_44)

cat_filter5 = data.filter(lambda x: x.category == 'Entertainment').cache()
rate_bigger_45 = cat_filter5.filter(lambda x: x.rate >= 4).cache()
rate_less_45 = cat_filter5.filter(lambda x: x.rate < 4).cache()
mean_bigger_45 = rate_bigger_45.map(lambda x: x.views).sum() / rate_bigger_45.count()
mean_less_45 = rate_less_45.map(lambda x: x.views).sum() / rate_less_45.count()
print("View average bigger than 4 of Entertainment", mean_bigger_45)
print("View average less than 4 of Entertainment", mean_less_45)

