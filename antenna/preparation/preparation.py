import os
import pandas as pd
import os


def read_merge_antenna_flows(is_repeat, omit=True):
    print("Reading antenna data...\n")
    location = '/Volumes/Extreme SSD/Data - Location/Hummingbird_Location_Datas/Trial/'
    df_trans = pd.DataFrame()
    for f in os.listdir(location):
        if 'E_Cell_tower_usage_data_2020' in f:
            df_trans = df_trans.append(pd.read_csv(os.path.join(location, f),
                                                   sep="|", skiprows=0,
                                                   header=0, encoding='ISO-8859-1'))
        print("Dataset" + str(f) + " is read")

    df_trans = df_trans.drop(['Unnamed: 0', 'Unnamed: 17'], axis=1)
    df_trans = df_trans.iloc[1:, :]
    df_trans = df_trans.replace('                    ', 0)
    df_trans = df_trans.reset_index(drop=True)
    df_trans = df_trans.rename(columns=lambda x: x.strip())
    df_trans = df_trans[df_trans['segment_2'] != "--------------------"]
    df_trans[df_trans.columns[1:16]] = df_trans[df_trans.columns[1:16]].astype(int)
    tower = pd.read_csv(
        r"/Volumes/Extreme SSD/Data - Location/Hummingbird_Location_Datas/A_Cell_Tower_Locations/cell_city_district.txt",
        sep="|",
        header=0, encoding='ISO-8859-1')
    tower = tower.drop(['Unnamed: 0', 'Unnamed: 4'], axis=1)
    tower = tower.iloc[1:, :]
    tower = tower.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    tower = tower.rename(columns=lambda x: x.strip())
    tower = tower.rename(columns={'matcher': 'site_id'})
    tower['site_id'] = tower['site_id'].astype(int)
    df_trans = df_trans.merge(tower, on='site_id', how='left')
    print('There are {} cell towers in the dataset'.format(df_trans.site_id.nunique()))

    return df_trans

def read_antenna_flows(antenna_location):
    print("Reading antenna data...\n")
    df = pd.read_csv(antenna_location, sep="|", skiprows=0, header=0, encoding='ISO-8859-1')
    df = df.drop(['Unnamed: 0', 'Unnamed: 17'], axis=1)
    df = df.iloc[1:, :]
    df = df.replace('                    ', 0)
    df = df.reset_index(drop=True)
    df = df.rename(columns=lambda x: x.strip())
    df = df[df['segment_2'] != "--------------------"]
    df[df.columns[1:16]] = df[df.columns[1:16]].astype(int)
    tower = pd.read_csv(
        r"/Volumes/Extreme SSD/Data - Location/Hummingbird_Location_Datas/A_Cell_Tower_Locations/cell_city_district.txt",
        sep="|",
        header=0, encoding='ISO-8859-1')
    tower = tower.drop(['Unnamed: 0', 'Unnamed: 4'], axis=1)
    tower = tower.iloc[1:, :]
    tower = tower.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    tower = tower.rename(columns=lambda x: x.strip())
    tower = tower.rename(columns={'matcher': 'site_id'})
    tower['site_id'] = tower['site_id'].astype(int)
    df = df.merge(tower, on='site_id', how='left')
    print('There are {} cell towers in the dataset'.format(df.site_id.nunique()))
    return df

if __name__ == "__main__":
    df = read_antenna_flows('/Volumes/Extreme SSD/Data - Location/Hummingbird_Location_Datas/E_Cell_tower_usage_data/E_Cell_tower_usage_data_202001.txt')

