import pandas as pd

class DataHandler():
    """
    整理源数据，为特征工程前的输入数据
    """
    def __init__(self, filename, id_column, time_column, target_column):
        """
        指定分类列名、时间列名、预测目标列名
        :param filename:
        """
        self.filename = filename
        self.origin_data = None
        self.id_column = id_column
        self.time_column = time_column
        self.target_column = target_column
        self.load_data()

    def data_transform(self):
        """
        将预测目标信息、aoi信息（如果需要）合并
        :return:
        """
        df = self.target_handle()                   #(4890,4)
        aoi_collects = self.aoi_handle()            #(30,4)
        df = pd.merge(left=df, right=aoi_collects, how='left', on='zone_id')

        df[self.id_column] = df[self.id_column].map(lambda x: int(x.split('_')[1]))  # 将zone_id变为id
        df.sort_values(by=[self.id_column, self.time_column], ascending=(True, True),
                                                            inplace=True, ignore_index= True) # 必须加ignore_index，否则排序时会带上index
        print('处理好的数据为：')
        print(df.head())
        print(df.info())
        print(df.shape)
        print(df.columns)

        return df

    def load_data(self):
        """
        加载原始数据
        :return:
        """
        self.origin_data = pd.read_csv(self.filename)
        print('已读取数据：')
        print(self.origin_data.head())
        print(self.origin_data.info())

    def target_handle(self):
        """
        ①展开为数据透视表，格式：id、time、target;
        ②利用3sigma法则处理异常值；
        ③处理好后对某些日的缺失值进行填充。(用周均值）
        :return:
        """
        # 展开为数据透视表
        target_collects = pd.pivot_table(data = self.origin_data,
                                            index = self.id_column,
                                            columns = self.time_column,
                                            values = self.target_column,
                                            aggfunc = 'sum')
        target_collects = target_collects.stack()               # zone_id, date为index，columns = ['pai_num','shou_num']
        target_collects = target_collects.reset_index()         # 将zone_id，date的index变为列

        # target_collects中若有zone的收/派件缺失，则缺少这一行的数据，此时，将日期补齐（主要在5月13日有这一情况）
        date = pd.Series(target_collects[self.time_column].unique(), name=self.time_column)  # 所有日期
        all_date_target = pd.DataFrame()
        for zone_id, groups in target_collects.groupby('zone_id'):
            groups = pd.merge(date, groups, how='left', on='date')
            groups.loc[groups['zone_id'].isna(), 'zone_id'] = zone_id
            all_date_target = pd.concat([all_date_target, groups], axis=0)
        all_date_target.reset_index(drop=True, inplace=True)
        print(all_date_target.info())

        # 生成weekday,为填充做准备
        dt_format = '%Y%m%d'
        all_date_target['date'] = all_date_target['date'].astype(str)
        all_date_target['date'] = pd.to_datetime(all_date_target['date'], format=dt_format)
        all_date_target['weekday'] = all_date_target['date'].dt.weekday

        # 对各zone，若一天的值，大于其均值+3倍标准差，或小于其均值-3倍标准差，则为异常值，用丢弃该值后该异常值所在的weekday的均值填充
        target_collects = pd.DataFrame()
        for zone_id, groups in all_date_target.groupby('zone_id'):
            groups.reset_index(drop=True, inplace=True)

            for target_column in ['pai_num', 'shou_num']:
                mean = groups[target_column].mean()
                std = groups[target_column].std()
                groups.loc[(groups[target_column] >= mean + 3 * std) | (
                            groups[target_column] <= mean - 3 * std), target_column] = None
                index_all = groups.loc[groups[target_column].isna(), target_column].index

                for index in index_all:
                    weekday = groups.loc[index, 'weekday']  # 为什么返回了Series
                    mean = groups.loc[groups['weekday'] == weekday, target_column].mean()
                    groups.loc[index, target_column] = round(mean, 0)

            target_collects = pd.concat([target_collects, groups], axis=0)

        target_collects.reset_index(drop=True, inplace=True)
        target_collects.drop('weekday',axis=1, inplace = True)

        return target_collects

    def aoi_handle(self):
        """
        关于aoi属性的处理：
        补齐全aoi与zone_id的对应关系后，取各zone的aoi数目、aoi类型数、aoi面积总和作为zone的属性
        :return:
        """
        aoi_data = pd.read_csv('data/aoi信息.csv')
        aoi_collects = pd.pivot_table(data=aoi_data,
                                      index='zone_id',
                                      values=['aoi面积', 'aoi_id', 'aoi类型'],
                                      aggfunc={'aoi面积': 'sum',
                                               'aoi_id': 'count',
                                               'aoi类型': lambda x: len(x.unique())})
        aoi_collects.columns = ['aoi_num', 'aoi_type', 'aoi_area']
        aoi_collects.reset_index(inplace=True)

        return aoi_collects

    @staticmethod
    def predict_handle(filepath):
        """
        读入处理好的预测数据
        :return:
        """
        pre_data = pd.read_csv(filepath)
        dt_format = '%Y%m%d'
        pre_data['date'] = pre_data['date'].astype(str)
        pre_data['date'] = pd.to_datetime(pre_data['date'], format=dt_format)

        return pre_data

if __name__ == '__main__':

    dataset = DataHandler('../data/quantity_train.csv', id_column ='zone_id',
                          time_column = 'date',
                          target_column = ['pai_num','shou_num'])
    data = dataset.data_transform()
    # data.to_csv('handled_data.csv')
    print(data.info)
    print(data.head())
    print(data.columns)
    print(data.shape)

    pre_data = DataHandler.predict_handle()
    print(pre_data.head())















