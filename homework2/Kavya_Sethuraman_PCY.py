from __future__ import division
import sys
import os
import csv

if __name__ == "__main__":
    input_file = sys.argv[1]
    support = int(sys.argv[5])
    num_of_buckets = int(sys.argv[4])
    path = sys.argv[6]
    if not os.path.exists(path):
        os.makedirs(path)
    filename = "frequentset" + '.txt'
    cand_file = "candidates" + '.txt'
    hash_a = int(sys.argv[2])
    hash_b = int(sys.argv[3])
    file_read = open(input_file).read()
    split_file = file_read.split()
    buckets = []
    for x in split_file:
        buckets.append(x.split(','))
    initial_candidatelist = []
    ind_freq_list = []

    for i in range(0, len(buckets)):
        for b in buckets[i]:
            initial_candidatelist.append(b)

    initial_candidatelist = sorted(set(initial_candidatelist))
    c_cnt = 0
    for k in initial_candidatelist:
        for i in range(0, len(buckets)):
            for j in buckets[i]:
                if (k == j):
                    c_cnt += 1
        if c_cnt >= support:
            ind_freq_list.append(k)

        c_cnt = 0

    itemiddic = {}
    counter = 1
    ind_freq_list_print = ind_freq_list
    for c in initial_candidatelist:
        itemiddic[c] = counter
        counter += 1
    with open(os.path.join(path, filename), 'wb') as temp_file:
        if ind_freq_list:
            file_writer = csv.writer(temp_file)
            # print ind_freq_list_print
            # for i  in range(0,len(ind_freq_list_print)):
            #     ind_freq_list_print[i]=int(ind_freq_list_print[i])

            for x in sorted(ind_freq_list):
                print x
                temp_file.write(str(x))
                temp_file.write("\n")
            countofbuckets = [0] * num_of_buckets
            bitmap = [0] * num_of_buckets
            freq_pairs = []

            for i in range(0, len(buckets)):
                for x in range(0, len(buckets[i]) - 1):
                    for y in range(x + 1, len(buckets[i])):
                        if (buckets[i][x] < buckets[i][y]):
                            print buckets[i][x]
                            print buckets[i][y]
                            print i,x,y
                            countofbuckets[((hash_a * int(str(itemiddic[buckets[i][x]]))) + (
                            hash_b * int(str(itemiddic[buckets[i][y]])))) % num_of_buckets] += 1
                            if ([buckets[i][x], buckets[i][y]] not in freq_pairs):
                                freq_pairs.append(sorted([buckets[i][x], buckets[i][y]]))
                        # else:
                        #     countofbuckets[((hash_a * int(str(itemiddic[buckets[i][y]]))) + (
                        #     hash_b * int(str(itemiddic[buckets[i][x]])))) % num_of_buckets] += 1
                        #     if ([buckets[i][y], buckets[i][x]] not in freq_pairs):
                        #         freq_pairs.append(sorted([buckets[i][y], buckets[i][x]]))

            freq_pairs = sorted(freq_pairs)

            for x in range(0, len(countofbuckets)):
                if countofbuckets[x] >= support:
                    bitmap[x] = 1
                else:
                    bitmap[x] = 0

            prunedpairs = []

            for i in range(0, len(freq_pairs)):
                for j in range(0, len(freq_pairs[i]) - 1):
                    if (freq_pairs[i][j] in ind_freq_list  and freq_pairs[i][j + 1] in ind_freq_list):
                        prunedpairs.append(freq_pairs[i])

            final_candidatelist = []

            "Checking condition 2 of PCY Pass 2"
            for i in range(0, len(prunedpairs)):
                for j in range(0, len(prunedpairs[i]) - 1):
                    if bitmap[((hash_a * int(str(itemiddic[prunedpairs[i][j]])) + (
                        hash_b * int(str(itemiddic[prunedpairs[i][j + 1]]))))) % num_of_buckets] == 1:
                        final_candidatelist.append(prunedpairs[i])

            with open(os.path.join(path, cand_file), 'a') as candfile:
                for i in final_candidatelist:
                    #candfile.write("\n")
                    candfile.write(str(i))
            candfile.close()

            pair_freq_list = []
            p = 0
            for c in range(0, len(final_candidatelist)):
                for b in range(0, len(buckets)):
                    if set(final_candidatelist[c]).issubset(set(buckets[b])):
                        p += 1
                print "len"
                #print len(buckets)
                if p >= support and p != 0:
                    pair_freq_list.append(sorted(final_candidatelist[c]))
                print len(pair_freq_list)
                p = 0
            print num_of_buckets
            pair_freq_list = sorted(pair_freq_list)
            print "fpr"
            x = len(pair_freq_list)
            fpr = (float)(x/num_of_buckets)
            print fpr

            with open(os.path.join(path, filename), 'a') as temp_file:
                if pair_freq_list:
                    print("\nFrequent Itemsets of size 2")
                    file_writer = csv.writer(temp_file)
                    for b in sorted(pair_freq_list):
                        file_writer.writerow(b)
    temp_file.close()

