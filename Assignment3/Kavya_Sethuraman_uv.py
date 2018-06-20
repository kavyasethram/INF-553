import sys
import numpy as np
import math
import pandas as pd

if __name__ == "__main__":

    file_to_read = sys.argv[1]
    n = int(sys.argv[2])
    m = int(sys.argv[3])
    f = int(sys.argv[4])
    it = int(sys.argv[5])
    out1= np.genfromtxt(file_to_read, delimiter=',',skip_header=1,usecols=(0, 1,2),dtype=(int, int, float),names=["a", "b", "c"])
    A = []
    B = []
    for i in range( 0, len( out1 ) ):
        A.append(out1[i][0])
        B.append(out1[i][1])
    B.sort()
    A.sort()

    A1 = []
    A1.append(A[0])
    j=0
    for i in range (1, len (A)):
        if ( A1[j] != A[i] ) :
            A1.append(A[i])
            j = j +1

    B1 = []
    B1.append(B[0])
    j=0
    for i in range (1, len (B)):
        if ( B1[j] != B[i] ) :
            B1.append(B[i])
            j = j +1

    mat = np.empty( shape=(n,m) ) * np.nan
    np.set_printoptions(suppress=True)
    for i in range( 0, len( out1 ) ):
        j=0;
        k=0;
        while(out1[i][0] != A1[j]):
            j=j+1


        while (out1[i][1] != B1[k]):
            k = k + 1
        mat[j][k] = format(out1[i][2],'0.2f')

    U = np.ones(shape=(n, f))
    V = np.ones(shape=(f, m))
    for z in range(it):
        for r in range(n):
            Mtemp = np.array(mat[r, :])
            for s in range(f):

                tmpsum = 0
                tmpd = 0
                Vtemp = np.array(V[s,:])
                Vtemp[np.isnan(Mtemp)] = np.nan
                tmpd = np.nansum(np.square(Vtemp))
                tmp_prod = Mtemp-np.dot(U[r,:],V)+ U[r][s]*V[s,:]
                tmpsum = np.nansum(V[s, :] * tmp_prod)
                if tmpd == 0:
                    U[r][s] = 0
                else:
                    U[r][s] = float(tmpsum) / tmpd

        for s in range(m):
            Mtemp = np.array(mat[:,s])
            for r in range(f):
                tmpsum = 0
                tmpd = 0
                Utemp = np.array(U[:,r])
                Utemp[np.isnan(Mtemp)] = np.nan
                tmpd = np.nansum(np.square(Utemp))
                tmp_prod = Mtemp - np.dot(U, V[:,s]) + U[:,r]*V[r, s]
                tmpsum = np.nansum(U[:, r] * tmp_prod)
                #print tmpsum
                if tmpd == 0:
                    V[r][s] = 0
                else:
                    V[r][s] = float(tmpsum) / tmpd

        approx = np.dot(U, V)
        error = 0
        cnt = 0
        for i in range(n):
            for j in range(m):
                if mat[i][j] > 0:
                    error += math.pow(mat[i][j] - approx[i][j],2)
                    cnt += 1
        error /= float(cnt)
        error = math.sqrt(error)
        print "%.4f" % error
