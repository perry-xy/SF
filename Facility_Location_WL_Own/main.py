from core.datahandle import DemandVisualization
import pandas as pd
import os

class Config():
    """
    模型调节参数
    """
    # to_B或to_C计算模式
    distribution_toB = True
    real_toB = False  # 先启动distribution_toB; True: to_B方式模式二，False：to_B方式模式三

    #固定参数
    YEAR_DAY=365
    num_rdc=None
    area_ratio=None
    weight_avg=12.5
    inventory_ratio = 1 #库存面积的比例，租用仓库时需要租大一点

    #是否有指定仓
    rdc_use_constr_open = True
    if rdc_use_constr_open:
        rdc_use = ['760','28','22','572','711','24']
        # rdc_use = ['711']
    cdc_use_constr_open = False
    if rdc_use_constr_open:
        cdc_use = []

if Config.distribution_toB:
    from core.facility_location_toB import FacilityLocation
    from core.result_format_toB import ResultFormat
    print("使用to_B的模式计算。")
else:
    from core.facility_location_toC import FacilityLocation
    from core.result_format_toC import ResultFormat
    print("使用to_C的模式计算。")

filename='data/input3.0.xlsx'
if filename ==  'data/input2.0.xlsx':
    print('以21年的数据进行计算。')
elif filename ==  'data/input3.0.xlsx':
    print('以20年的数据进行计算。')
else:
    print('所计算的数据的年份未知。')

data_input=DemandVisualization(filename, Config)
#data_input.demandvisual()  #是否画需求分布图
df_performance = pd.DataFrame()
rdc_select = pd.DataFrame()
filepath = '沈阳16块'
if not os.path.exists('{}/'.format(filepath)):
    os.mkdir('{}'.format(filepath))

# for warehouse_num in range(1,16):
for warehouse_num in [6]:
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

    rdc_select = rdc_select.append(df_rdc.loc[:,'City_name'].T)

df_performance.to_csv('{}/performance.csv'.format(filepath), encoding = 'utf-8_sig',index=False)
rdc_select.index = list(range(warehouse_num - len(rdc_select) + 1,warehouse_num + 1))
rdc_select.to_csv('{}/rdc_select.csv'.format(filepath), encoding = 'utf-8_sig')

