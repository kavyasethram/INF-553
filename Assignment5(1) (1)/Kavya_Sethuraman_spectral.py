import networkx as net
import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt

InputFile=sys.argv[1]
k = int(sys.argv[2])
Outputfile = sys.argv[3]
labels = []
edge_list = []
reader = np.loadtxt (InputFile , dtype=int)
for row in reader:
      edge_list.append(tuple(row.tolist()))
      labels.extend(row)
labels = [int(i) for i in labels]
labels = sorted(set(labels))

mylabels = []
myedge_list = []
max_clus = []

mylabels=labels
print mylabels
myedge_list=edge_list
print "edges"
print myedge_list
print len(myedge_list)
cnt = 1
final_clus = []
while(cnt < k):
      print cnt
      if len(max_clus)>0:
            final_clus.remove( max_clus )
      mygraph = net.Graph()
      mygraph.add_nodes_from( mylabels )
      mygraph.add_edges_from( myedge_list )
      nodelist = sorted( mygraph.nodes() )
      adj_matrix = net.adjacency_matrix( mygraph, nodelist )
      print adj_matrix
      am = pd.DataFrame( adj_matrix.todense() )
      print am
      print adj_matrix
      am.columns=mylabels
      am.index=mylabels
      am.index=mylabels


      D = np.diag(np.ravel(np.sum(adj_matrix,axis=1)))

      L=D-adj_matrix
      l, U = np.linalg.eigh(L)
      f = U[:,1]
      cluster1 = []
      cluster2 = []

      for i in range(len(f)):
            if f[i] < 0:
                  cluster1.append(i)
            else:
                  cluster2.append(i)

      cluster1 = [mylabels[j] for j in cluster1]
      cluster2 = [mylabels[l] for l in cluster2]
      # print "Cluster 1"
      # print len(cluster1),cluster1
      # print "Cluster 2"
      # print len(cluster2),cluster2
      final_clus.append(cluster1)
      final_clus.append(cluster2)
      max_clus = max(final_clus, key=len)
      # print "Selected Cluster"
      # print len(max_clus),max_clus
      mylabels=sorted(max_clus)
      myedge_list = []
      for i in range(len(max_clus)):
            for j in range(len(max_clus)):
                  if i != j:
                        a=max_clus[i]
                        b=max_clus[j]

                        if am[a][b] == 1:
                              print "a,b"
                              print a, b
                              myedge_list.append(tuple([a,b]))

      adj_matrix=np.zeros( [len(labels),len(labels)])
      cnt += 1

print len(final_clus)
print len(final_clus[0])
print final_clus[0]
print len(final_clus[1])
print final_clus[1]
print len(final_clus[2])
print final_clus[2]

myfile = open( Outputfile, "w" )
for m in range(len(final_clus)):
      if (m != 0):
            myfile.write( "\n" )
      for item in final_clus[m]:
            myfile.write(str(item))
            if len(final_clus[m]) != final_clus[m].index(item)+1:
                  myfile.write(",")
myfile.close()
