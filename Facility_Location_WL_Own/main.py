from core.datahandle import DemandVisualization
from core.facility_location import FacilityLocation
from core.result_format import ResultFormat
import pandas as pd
import os

class Config():
    """
    模型调节参数
    """
    #固定参数
    YEAR_DAY=365
    num_rdc=None
    area_ratio=None
    weight_avg=12.5
    inventory_ratio = 1 #库存面积的比例，租用仓库时需要租大一点

    #是否有指定仓
    rdc_use_constr_open = False
    if rdc_use_constr_open:
        rdc_use = ['24','28','572','769','27','22']
    cdc_use_constr_open = False
    if rdc_use_constr_open:
        cdc_use = []

filename='data/input.xlsx'
data_input=DemandVisualization(filename)
#data_input.demandvisual()  #是否画需求分布图
df_performance = pd.DataFrame()
filepath = '1.5_0.2_area_ratio'
if not os.path.exists('{}/'.format(filepath)):
    os.mkdir('{}'.format(filepath))

for warehouse_num in range(1,15):
#for warehouse_num in [3,4,5,6,7]:
    Config.num_rdc=warehouse_num
    Config.area_ratio=data_input.warehouse_area_ratio[warehouse_num]

    model=FacilityLocation(data_input,Config)
    model.facility_location()

    df_rdc=ResultFormat(model).rdc_post_process()
    df_rdc.to_csv('{}/RDC_{}.csv'.format(filepath,warehouse_num), encoding = 'utf-8_sig', index=False)

    df_c_network=ResultFormat(model).c_end_network()
    df_c_network.to_csv('{}/C_Network_{}.csv'.format(filepath,warehouse_num), index=False)

    df_cdc=ResultFormat(model).cdc_rdc_network()
    df_cdc.to_csv('{}/CDC_Network_{}.csv'.format(filepath,warehouse_num), index=False)

    df_cdc_handle,cdc_unuse=ResultFormat(model).handle_cdc_work(df_cdc)
    df_cdc_handle.to_csv('{}/Handle_CDC_Network_{}.csv'.format(filepath,warehouse_num), encoding = 'utf-8_sig', index=False)
    cdc_unuse.to_csv('{}/CDC_Unuse_{}.csv'.format(filepath,warehouse_num), encoding = 'utf-8_sig', index=False)

    performance=ResultFormat(model).performance(warehouse_num)
    df_performance=df_performance.append(performance)

df_performance.to_csv('{}/performance.csv'.format(filepath), encoding = 'utf-8_sig',index=False)


