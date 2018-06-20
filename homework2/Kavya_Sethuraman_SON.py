import sys,csv
from pyspark import SparkContext
import itertools
import collections
candidate_set = set()
global_frequent_set = []
def calculate_frequent_item_sets(baskets_list):
    item_set = set()
    baskets = []
    frequent_item_sets = set()
    for transaction in baskets_list:
        items = transaction.split(',')
        baskets.append(items)
        for item in items:
            item_set.add([item])
    one_frequent_item_set = filter_items_with_minimum_threshold(
        item_set, baskets,support_threshold)
    frequent_item_sets.update(one_frequent_item_set)
    current_frequent_set = one_frequent_item_set
    count = 2
    while len(one_frequent_item_set) > 0:
        candidate_set = construct_candidate_sets(
            one_frequent_item_set, count)
        one_frequent_item_set = filter_items_with_minimum_threshold(
            candidate_set, baskets,support_threshold)
        frequent_item_sets.update(one_frequent_item_set)
        count += 1
    return frequent_item_sets

def construct_candidate_sets(frequent_set,kth):
    new_candidate_set = set()
    for item_k in frequent_set:
        for item_j in frequent_set:
            new_candidate = item_k.union(item_j)
            if len(new_candidate) == kth:
                new_candidate_set.add(new_candidate)
    return new_candidate_set

def find_global_frequent_sets(transaction_list):
    candidate_set_count = collections.defaultdict(int)
    for transaction in transaction_list:
        basket = transaction.split(',')
        for candidate in candidate_set:
            if candidate.issubset(basket):
                candidate_set_count[candidate] += 1
    yield candidate_set_count

def filter_items_with_minimum_threshold(item_set, baskets,s_threshold):
    support_threshold = s_threshold * len(baskets)
    item_counts = collections.defaultdict(int)
    frequent_item_set = set()
    for basket in baskets:
        for item in item_set:
            if item.issubset(basket):
                item_counts[item] += 1
    for item, count in item_counts.items():
        if count >= support_threshold:
            frequent_item_set.add(item)
    return frequent_item_set

if __name__ == "__main__":
    input_file = sys.argv[1]
    support_threshold = float(sys.argv[2])
    output_file = sys.argv[3]
    sc = SparkContext(appName="hw2")
    rdd = sc.textFile(input_file)
    candidate_set = rdd.mapPartitions(calculate_frequent_item_sets).collect()
    c = sc.parallelize(rdd.mapPartitions(find_global_frequent_sets).collect()).flatMap(
        lambda x: x.items()).reduceByKey(lambda x, y: x + y).collect()
    rdd1 = rdd.map(lambda line: line.split("\r\n"))
    s_T = support_threshold * rdd1.count()
    for item, v in c:
        if v >= s_T:
            global_frequent_set.append(sorted(list(item)))

    global_frequent_set = [list(map(int, i)) for i in global_frequent_set]
    for i in range(0,len(global_frequent_set)):
        global_frequent_set[i] = sorted(global_frequent_set[i])
    global_frequent_set = sorted(global_frequent_set)
    ind_globl_freq_set = []
    rem_globl_freq_set = []
    for i in range(0,len(global_frequent_set)):
        if len(global_frequent_set[i] == 1):
            ind_globl_freq_set.append(global_frequent_set[i])
        else:
            rem_globl_freq_set.append(global_frequent_set[i])

    output_file = open(output_file, "w")
    global_frequent_set = sorted(global_frequent_set)
    global_frequent_set = [item for value in global_frequent_set for item in literal_eval(value)]
    for frequent_items in ind_globl_freq_set:
        file_writer.writerow(frequent_items)
    for freq_rw in rem_globl_freq_set:
        for i in range(2,maxlen):
            if len(freq_rw) == i:
                file_writer.writerow(freq_rw)
    output_file.close()

