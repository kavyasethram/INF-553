from __future__ import division
import sys
import os
import csv

if __name__ == "__main__":
    input_file = sys.argv[1]
    hash_a = int(sys.argv[2])
    hash_b = int(sys.argv[3])	
    support = int(sys.argv[5])
    num_of_buckets = int(sys.argv[4])
    path = sys.argv[6]
    if not os.path.exists(path):
        os.makedirs(path)
    filename = "frequentset" + '.txt'
    cand_file = "candidates" + '.txt'
    mybuckets = []
    initial_candidatelist = []

    file_read = open(input_file).read()
    # split_file = file_read.split()
    # for i in split_file:
    #
    #     mybuckets.append(i.split(','))
    #     list_transaction = list(map(int, list_transaction))

    file = open(input_file, 'r')
    for i in file:
        i = i.strip().split(',')
        i = list(map(int, i))
        mybuckets.append(i)

    for j in range(0, len(mybuckets)):
        for k in mybuckets[j]:
            initial_candidatelist.append(k)

    initial_candidatelist = sorted(set(initial_candidatelist))
    cnt = 1
    itemise = {}
    for p in initial_candidatelist:
        itemise[p] = cnt
        cnt += 1

    c_cnt = 0
    ind_freq_list = []
    for m in initial_candidatelist:
        for n in range(0, len(mybuckets)):
            for o in mybuckets[n]:
                if (m == o):
                    c_cnt += 1
        if c_cnt >= support:
            ind_freq_list.append(m)
        c_cnt = 0
    ind_freq_list_print = []
    ind_freq_list_print = ind_freq_list

    bitmap = [0] * num_of_buckets
    bucketscnt = [0] * num_of_buckets
    freq_pairs = []
    for r in range(0, len(mybuckets)):
        for s in range(0, len(mybuckets[r]) - 1):
            for t in range(s + 1, len(mybuckets[r])):
                if (mybuckets[r][s] < mybuckets[r][t]):
                    bucketscnt[((hash_a * itemise[mybuckets[r][s]]) + (
                    hash_b * (itemise[mybuckets[r][t]]))) % num_of_buckets] += 1
                    #print val
                    if ([mybuckets[r][s], mybuckets[r][t]] not in freq_pairs):
                        freq_pairs.append(sorted([mybuckets[r][s], mybuckets[r][t]]))
                else:
                    # print "else"
                    # print mybuckets[r][t],mybuckets[r][s]
                    bucketscnt[((hash_a * itemise[mybuckets[r][t]]) + (
                    hash_b * itemise[mybuckets[r][s]]) % num_of_buckets)] += 1
                    if ([mybuckets[r][t], mybuckets[r][s]] not in freq_pairs):
                        freq_pairs.append(sorted([mybuckets[r][t], mybuckets[r][s]]))

    #temp_file.close()
    freq_pairs = sorted(freq_pairs)
    #print freq_pairs
    #print freq_pairs
    for a in range(0, len(bucketscnt)):
        if bucketscnt[a] >= support:
            bitmap[a] = 1
        else:
            bitmap[a] = 0
    #
    # for b in range(0, len(freq_pairs)):
    #     print freq_pairs[b]
    ppairs = []
    for b in range(0, len(freq_pairs)):
        for c in range(0, len(freq_pairs[b]) - 1):
            if (freq_pairs[b][c] in ind_freq_list  and freq_pairs[b][c + 1] in ind_freq_list):
                ppairs.append(freq_pairs[b])

    final_candidatelist = []
    cclist = []
    num_freq_items = 1
    for d in range(0, len(ppairs)):
        for e in range(0, len(ppairs[d]) - 1):
            if bitmap[((hash_a * int(str(itemise[ppairs[d][e]])) + (
                hash_b * int(str(itemise[ppairs[d][e + 1]]))))) % num_of_buckets] == 1:
                final_candidatelist.append(ppairs[d])
    temp_list = []
    for i in range(0,len(bitmap)):
        if bitmap[i] == 1:
            num_freq_items+= 1
        if bitmap[i] == 0:
            temp_list.append(i)
    #print temp_list

    # for d in range(0, len(freq_pairs)):
    #     for e in range(0, len(freq_pairs[d]) - 1):
    #         if ((hash_a * int(str(itemise[freq_pairs[d][e]])) + (
    #             hash_b * int(str(itemise[freq_pairs[d][e + 1]]))))) % num_of_buckets in temp_list:
    #             cclist.append(freq_pairs[d])
    # print cclist

    # for d in range(0, len(ppairs)):
    #     for e in range(0, len(ppairs[d]) - 1):
    #         if (hash_a * int(str(itemise[ppairs[d][e]])) + (
    #                     hash_b * int(str(itemise[ppairs[d][e + 1]])))) % num_of_buckets in temp_list:
    #             cclist.append(ppairs[d])
    # print cclist
    #             # num_freq_items+=1



    with open(os.path.join(path, cand_file), 'a') as candfile:
        for f in final_candidatelist:
            candfile.write("\n")
            candfile.write(str(f))
    candfile.close()

    pair_freq_list = []
    g = 0
    for h in range(0, len(final_candidatelist)):
        for i in range(0, len(mybuckets)):
            if set(final_candidatelist[h]).issubset(set(mybuckets[i])):
                g += 1
        if g >= support and g != 0:
            pair_freq_list.append(sorted(final_candidatelist[h]))
        g = 0

    cand_list_p = [a for a in freq_pairs if
                   (a in freq_pairs) and (a not in pair_freq_list)]
    #print cand_list_p

    cand_list_p = [list(map(int, i)) for i in cand_list_p]

    for d in range(0, len(cand_list_p)):
        for e in range(0, len(cand_list_p[d]) - 1):
            if (hash_a * cand_list_p[d][e] +
                        hash_b * cand_list_p[d][e + 1]) % num_of_buckets in temp_list:
                    if(cand_list_p[d][e] in ind_freq_list_print and cand_list_p[d][e+1] in ind_freq_list_print):
                        cclist.append(cand_list_p[d])
    #print cclist
    fpr = (float)(num_freq_items/num_of_buckets)
    fpr = "%.3f" % fpr
    print "False Positive Rate:"+str(fpr)
    new_list = [list(map(int, i)) for i in pair_freq_list]
    for i in range(0,len(new_list)):
        if new_list[i][0] > new_list[i][1]:
            new_list[i][0],new_list[i][1] = new_list[i][1],new_list[i][0]
    new_list = sorted(new_list)

    for ii in ind_freq_list_print:
        ind_freq_list_print[ind_freq_list_print.index( ii )] = int( ii )
        ind_freq_list_print.sort()

    cclist = [list(map(int, i)) for i in cclist]
    for i in range(0, len(cclist)):
        if cclist[i][0] > cclist[i][1]:
            cclist[i][0], cclist[i][1] = cclist[i][1], cclist[i][0]
    cclist = sorted(cclist)

    cclist = [tuple(l) for l in cclist]

    with open(os.path.join(path, cand_file), 'wb') as candfile:
        for f in cclist:
            candfile.write(str(f))
            candfile.write("\n")
    candfile.close()

    new_list = [tuple(l) for l in new_list]

    with open(os.path.join(path, filename), 'wb') as temp_file:
        for q in ind_freq_list_print:
            temp_file.write(str(q))
            temp_file.write("\n")
        for j in new_list:
            temp_file.write(str(j))
            temp_file.write( "\n" )
    temp_file.close()