import pandas as pd
import numpy as np

def residential_inclusion(antenna_location):
    ##### Antenna dataset ####
    df = read_antenna_flows(antenna_location)
    tower = read_tower_data(tower_location)
    df = df.merge(tower, on='site_id', how='left')
    df = df[(df['time'].dt.hour >= 20) | (df['time'].dt.hour <= 8)].reset_index(drop=True)
    ##### We only look at people who use these towers at night #####
    group = df.groupby([df['time'].dt.month.rename('month'),df['site_id'],df['city'],df['district']]).mean().round(decimals=0).reset_index()
    group['total_migrant']=group[group.columns[4:16]].sum(axis=1)
    group['total_locals'] = group[group.columns[16:18]].sum(axis=1)
    group['total_refugee'] = group['segment_1'] + group['segment_12']
    group = pd.pivot_table(group, values=['total_migrant', 'total_locals'], index=['month', 'city', 'district'],
                           aggfunc=np.sum).reset_index()
    group['residential_inclusion']=group['total_migrant']/(group['total_migrant']+group['total_locals'])
    return group