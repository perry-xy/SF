from datahandle import DemandVisualization
from facility_location import FacilityLocation
from result_format import ResultFormat
import pandas as pd

class Config():
    """
    模型调节参数
    """
    #固定参数
    YEAR_DAY=365
    num_rdc=None
    area_ratio=None
    weight_avg=350

    #是否有指定仓
    rdc_use_constr_open = False
    if rdc_use_constr_open:
        rdc_use = ['24','28','711','572','760','311','531']
    cdc_use_constr_open = False
    if rdc_use_constr_open:
        cdc_use = []

filename='input.xlsx'
data_input=DemandVisualization(filename)
#data_input.demandvisual()
df_performance = pd.DataFrame()

for warehouse_num in range(1,4):
#for warehouse_num in [1]:
    Config.num_rdc=warehouse_num
    Config.area_ratio=data_input.warehouse_area_ratio[warehouse_num]

    model=FacilityLocation(data_input,Config)
    model.facility_location()

    df_rdc=ResultFormat(model).rdc_post_process()
    df_rdc.to_csv('RDC_{}_t.csv'.format(warehouse_num), encoding = 'utf-8_sig')

    df_c_network=ResultFormat(model).c_end_network()
    df_c_network.to_csv('C_Network_{}_t.csv'.format(warehouse_num), index=False)

    df_cdc=ResultFormat(model).cdc_rdc_network()
    df_cdc.to_csv('CDC_Network_{}_t.csv'.format(warehouse_num), index=False)

    df_cdc_handle,cdc_unuse=ResultFormat(model).handle_cdc_work(df_cdc)
    df_cdc_handle.to_csv('Handle_CDC_Network_{}_t.csv'.format(warehouse_num), encoding = 'utf-8_sig')
    cdc_unuse.to_csv('CDC_Unuse_{}_t.csv'.format(warehouse_num), encoding = 'utf-8_sig')

    performance=ResultFormat(model).performance(warehouse_num)
    df_performance=df_performance.append(performance)

df_performance.to_csv('performance_t.csv', encoding = 'utf-8_sig')


