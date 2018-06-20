import sys
import numpy as np
import math
import pandas as pd
import math


if __name__ == "__main__":
    global out1

    sample_file = sys.argv[1]
    actual_file = sys.argv[2]
    k = int(sys.argv[3])
    n = int(sys.argv[4])
    p = float(sys.argv[5])
    #p = float(p/100)
    out1 = np.genfromtxt(sample_file, delimiter=',')
    out1 = np.asarray(out1)

    dis_mat = (np.sum((out1[None, :] - out1[:, None]) ** 2, -1) ** 0.5)

    copy_mat = dis_mat
    np.fill_diagonal(dis_mat, np.inf)

    i = len(out1)
    clusters = []
    for i in range(len(out1)):
        clusters.append([i])

    ind = []
    while i >= k:
        dis_ind = np.argmin((dis_mat))
        dis_ind = np.unravel_index(dis_ind,dis_mat.shape)

        clusters[dis_ind[0]].extend( clusters[dis_ind[1]] )
        clusters.remove( clusters[dis_ind[1]] )

        dis_mat[:,dis_ind[0]] = np.minimum(dis_mat[:,dis_ind[0]], dis_mat[:,dis_ind[1]])
        dis_mat[dis_ind[0],:] =  (np.minimum(dis_mat[dis_ind[0],:], dis_mat[dis_ind[1],:]))

        dis_mat = np.delete(dis_mat, dis_ind[1], axis=0)
        dis_mat = np.delete(dis_mat, dis_ind[1], axis=1)

        np.fill_diagonal(dis_mat, np.inf)

        orig_ind = np.argmin((dis_mat))
        orig_ind = np.unravel_index(dis_ind,dis_mat.shape)

        i-=1


centroid=[]
values = [[] for i in range(k)]
val = []


for i in range(len(clusters)):
    for j in range(len(clusters[i])):
        val.append(out1[clusters[i][j]])
    values[i].extend(val)
    x = np.sum(val,axis=0)/len(clusters[i])
    centroid.append(x)
    val = []

rep_points = []

for i in range(len(values)):
    x , y  = zip(*values[i])
    ind=np.argmin(x)
    rep_points.append(values[i][ind])


dist1 = []
max_distance = []
if n >1:
    for i in range(len(values)):
        dist1 = []
        x = values[i]
        for j in range(len(x)):
            dist1.append(math.sqrt((rep_points[i][0] - x[j][0]) ** 2 + (rep_points[i][1] - x[j][1]) ** 2))
        z = np.argmax(dist1)
        rep_points.append(x[z])

if n >=2:
    cl=len(clusters)
    pts=2
    for a in range(2,n):
        for i in range(len(values)):
            x = values[i]
            dist2 = []
            for k in range( 0, pts ):
                dist1 = []
                for j in range( len( x ) ):
                    dist1.append(math.sqrt((rep_points[i+cl*k][0] - x[j][0]) ** 2 + (rep_points[i+cl*k][1] - x[j][1]) ** 2))
                if k > 0:
                    dist2 = np.minimum( dist1, dist2)
                    q = np.argmax( dist2 )
                else:
                    dist2 = dist1
            rep_points.append( x[q] )
        pts +=1

cl_l =[]
for v in range(cl):
    #print "\nCluster",format(v)
    t=v
    cl_l.append( [] )
    for w in range(n):
        cl_l[v].append(rep_points[t].tolist())
        t = t + cl
    print cl_l[v]


rep_points_1 = []
j = 0
cnt = 0
final_rep_points = []
for i in range(len(clusters)):
    j = i
    while cnt < n:
        m = (float)(centroid[i][0] - rep_points[j][0])
        nn = (float)(centroid[i][1] - rep_points[j][1])
        rep_points_1.append([rep_points[j][0] + m*(p),rep_points[j][1] + nn*(p)])
        j = j+ len(clusters)
        cnt +=1
    cnt = 0
    j = i
    final_rep_points.append(rep_points_1)


out2 = np.genfromtxt(actual_file, delimiter=',')
out2 = np.asarray(out2)


minDist = 99999
f= open(sys.argv[6],"w+")

clstr_point = []
minDist = 9999
i =0
ii=0
x = final_rep_points[0]
for points in out2:
    i = 0
    ii = 0
    while i < len(x):
        for k in range(n):
            z=k+i
            dist = (math.sqrt((x[z][0] - points[0]) ** 2 + (x[z][1] - points[1]) ** 2))
            if dist < minDist:
                minDist = dist
                clustChoice = ii
        i = i+n
        ii = ii +1
    clstr_point.append([points,clustChoice])
    minDist = 9999
    f.write(str(points[0]) + "," + str(points[1]) + "," + str(clustChoice) + '\n')


