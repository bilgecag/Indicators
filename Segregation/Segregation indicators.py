import pandas as pd
import numpy as np

def residential_inclusion(df, period):
    ##### Antenna dataset ####
    df = df[(df['time'].dt.hour >= 20) | (df['time'].dt.hour <= 8)].reset_index(drop=True)
    group = df.groupby([df['time'].dt.month.rename('Month'),df['site_id'],df['city'],df['district']]).mean().round(decimals=0).reset_index(drop=True)
    group['total_migrant']=group[group.columns[4:16]].sum(axis=1)