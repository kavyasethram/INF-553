import re
import sys
import csv
from operator import add
from pyspark import SparkContext
from string import punctuation

sc=SparkContext(appName='hw3')


res1=sc.textFile(sys.argv[1]).map(lambda s:s.encode("ascii","ignore"))


header = res1.first()
res1= res1.filter(lambda line: line != header)
res1 = res1.map(lambda s: s[1:-1].split(","))

res=res1.map(lambda s: (str(s[3]).strip('').strip().replace("'","").replace("-","").replace("("," ").replace(")"," ").replace("?"," ").replace("$"," ").replace("["," ").replace("]"," ").replace("#"," ").replace("~"," ").replace("&"," ").replace("/"," ").replace("<"," ").replace("`"," ").replace("%"," ").replace("^"," ").replace("_"," ").replace("*"," ").replace("."," ").replace('"'," ").strip().lower(),int(s[18])))
res = res.filter(lambda u: u[0] !='')


res2 = res.aggregateByKey((0,0), lambda U,v: (U[0] + v, U[1] + 1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))
res2 = res2.map(lambda (x, (y, z)): (x, z,(float(y)/z))).sortByKey()

output2 = res2.collect()
res2 = res2.saveAsTextFile(sys.argv[2])



