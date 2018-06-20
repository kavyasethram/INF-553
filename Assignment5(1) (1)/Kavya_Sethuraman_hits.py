import re, sys,os
import math

from operator import add

from pyspark import SparkContext



dir_path = os.path.join(sys.argv[3])  # will return 'feed/address'

try:
    os.mkdir(dir_path, 0755)
except OSError:
    pass

def computeAuth(urls, hub):
    """Calculates hub contributions to the auth of other URLs."""
    num_urls = len(urls)
    for url in urls: yield (url, hub)

def computeHub(urls, auth):
    """Calculates auth contributions to the hub of other URLs."""
    num_urls = len(urls)
    for url in urls: yield (url, auth)

def outNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    #print type(int(parts[0]))
    return int(parts[0]), int(parts[1])

def inNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return int(parts[1]), int(parts[0])

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: pagerank <master> <file> <number_of_iterations>"
        exit(-1)

    # Initialize the spark context.
    sc = SparkContext(appName="HitsPageRank")

    lines = sc.textFile(sys.argv[1], 1)


    # Loads all URLs from input file and initialize their neighbors.
    out_links = lines.map(lambda urls: outNeighbors(urls)).distinct().groupByKey().cache()
    #print (out_links.collect())
    in_links = lines.map(lambda urls: inNeighbors(urls)).distinct().groupByKey().cache()
    #print (out_links.collect())

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    hubs = out_links.map(lambda (url, neighbors): (url, 1.0))
    #print hubs.collect()

    auths = in_links.map(lambda (url, neighbors): (url, 1.0))
    #print auths.collect()

    # Calculates and updates URL ranks continuously using Hits algorithm.
    for iteration in xrange(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        # Here we are contributing auth of a link present in the outgoing list of a link whose hub is given
        auth_contribs = out_links.join(hubs).flatMap(lambda (url, (urls, hub)):
            computeAuth(urls, hub))
        #print auth_contribs.collect()
        auths = auth_contribs.reduceByKey(add)
        #print (auths.collect())
        #print auths(lambda x:x[1])[1]
        max_value = max(auths.collect(), key=lambda x:x[1])[1]
        #print max_value
        auths = auths.mapValues(lambda rank: rank/(max_value))
        #print auths
        out1=auths.sortByKey(True).collect()
        #out1=out1
        #print out1
        #print auths
        # Here we are contributing hub of a link present in the incoming list of a link whose auth is given
        hub_contribs = in_links.join(auths).flatMap(lambda (url, (urls, auth)):
            computeHub(urls, auth))
        #print hub_contribs.collect()
        hubs = hub_contribs.reduceByKey(add)
        #print hubs.collect()
        max_value = max(hubs.collect(), key=lambda x:x[1])[1]
        hubs = hubs.mapValues(lambda rank:rank/(max_value)).sortByKey([True])
        out2=hubs.collect()
        #print hubs.collect()
        # Re-calculates URL ranks based on neighbor contributions.


    with open(os.path.join(dir_path,'authority.txt'),'w') as fw:
      for (link, rank) in out1:
         fw.write(str(link)+','+'{0:.5f}'.format(rank)+'\n')
    with open(os.path.join(dir_path,'hub.txt'),'w') as fw:
      for (link, rank) in out2:
         fw.write(str(link)+','+'{0:.5f}'.format(rank)+'\n')

