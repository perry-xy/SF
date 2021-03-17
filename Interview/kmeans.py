# @Time : 2021/2/3 6:12 上午 
# @Author : Xingyou Chen
# @File : kmeans.py 
# @Software: PyCharm
import numpy as np
import matplotlib.pyplot as plt

def random_data(size, u1, u2):
    cov = np.array([[1, 0], [0, 1]])
    data_1 = np.random.multivariate_normal([u1, u1], cov, int(size/2))
    data_2 = np.random.multivariate_normal([u2, u2], cov, int(size/2))
    data = np.vstack((data_1, data_2))
    return data

def loaddata(file):
    dataset = np.loadtxt(file, delimiter = '\t')
    return dataset

def distEclud(x, y):
    distance = np.sum((x- y)**2)
    return distance

def initcentroid(dataset, k):
    m , n = dataset.shape   # m：记录数，n：特征数
    initIndex = list()
    while len(initIndex) < k:
        index = int(np.random.uniform(0, m))
        if index not in initIndex:
            initIndex.append(index)
    centroids = dataset[initIndex,:]
    return centroids

def kmeans(dataset, k):
    m = dataset.shape[0]  # 样本数
    clusterAssment = np.zeros((m,2)) # 两列：第一列：簇标号，第二列：到簇中心的距离
    clusterChange = True

    centroids = initcentroid(dataset, k) # 生成初始簇中心
    print(centroids)

    while clusterChange:
        clusterChange = False

        # 遍历所有样本
        for i in range(m):
            mindistance = 1E10  # TODO：初始距离的设定的优化
            minindex = -1
            for j in range(k):  # 遍历所有簇中心
                distance = distEclud(centroids[j,:], dataset[i,:])
                if distance < mindistance:
                    mindistance = distance
                    minindex = j
            if clusterAssment[i,0] != minindex:
                clusterChange = True
                clusterAssment[i,:] = minindex, mindistance
            clusterAssment[i, 1] = mindistance   # 就算不改变所属簇，也将最小距离更新

        # 更新质心
        for i in range(k):
            centroids[i,:] = dataset[clusterAssment[:,0] == i,:].mean(axis = 0)
        print('TTT:')
        print(centroids)
        print(clusterAssment)

    print("Congratulations,cluster complete!")
    return centroids, clusterAssment

def plot_cluster(dataSet, clusterAssment, centroids, k):
    colors = ['red', 'blue', 'green', 'orange', 'yellow']
    for cluster_no in range(k):
        cluster_data = dataSet[clusterAssment[:,0] == cluster_no,:]
        plt.scatter(cluster_data[:,0], cluster_data[:,1], label = 'cluster{}'.format(cluster_no),
                    color = colors[cluster_no], marker = 'o')
        plt.scatter(centroids[cluster_no,0], centroids[cluster_no,1], color='black', marker='*')
    plt.legend(loc='upper left')
    plt.xlabel('x')
    plt.ylabel('y')
    plt.show()



if __name__ == '__main__':
    # dataSet = loaddata("kmeans.txt")
    dataSet = random_data(100, 1, 10)
    k = 2
    centroids,clusterAssment = kmeans(dataSet,k)
    print(dataSet)
    print(centroids)
    print(clusterAssment)
    plot_cluster(dataSet, clusterAssment, centroids, k)












