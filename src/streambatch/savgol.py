import pandas as pd
import numpy as np
from scipy.signal import savgol_filter

#
# Find outliers
# ndvi(t) is an outlier if:
# 1) abs(zscore) > 1.5 (relative to it's neighbours on each side)
# 2) it is "far" away from the mean in absolute terms too, i.e.
#    ndvi(t) - mean > 0.15
#
def find_outliers(m2):
    # compure zscore for every point using two points before and two points after each point
    window_size = 7
    rolling_mean = m2['ndvi'].rolling(window=window_size, center=True).mean()
    rolling_std = m2['ndvi'].rolling(window=window_size, center=True).std()

    # Calculate the z-score for each point
    m2['rolling mean'] = rolling_mean
    m2['rolling std'] = rolling_std
    m2['zscore'] = (m2['ndvi'] - rolling_mean) / rolling_std
    m2['delta'] = m2['ndvi'] - m2['rolling mean']
    m2['outlier'] = (m2['delta'].abs() > 0.15) & (m2['zscore'].abs() > 1.5)
    return m2

# m is a dataframe with columns time and ndvi
def remove_outliers(m):
    m1 = m.copy()
    m1 = find_outliers(m1)
    m2 = m1[m1['outlier']==False]
    # print(f"removed {m1.shape[0] - m2.shape[0]} outliers")
    # print("here")
    # print(m2.head())
    # remove the last 5 columns of m2
    m2 = m2.drop(columns=['rolling mean', 'rolling std', 'zscore', 'delta', 'outlier'])
    return m2

def handle_duplicates(m):
    # !!! a better solution is the choose the "better" observation based on before and after
    # find successive rows with the same time
    # if there are two rows with the same time, keep the first one
    m1 = m.copy()
    m1['duplicate'] = m1['time'].eq(m1['time'].shift(-1))
    m2 = m1[m1['duplicate']==False]
    # print(f"removed {m1.shape[0] - m2.shape[0]} duplicates")
    m2 = m2.drop(columns=['duplicate'])
    return m2

def prepare(df):
    # is this points or polygons?
    sort_col = 'point'
    if df.columns[0] != 'point':
        sort_col = 'location'
    df.sort_values(by=[sort_col, 'time'])
    # slice off all rows where qa.sentinel2 == 1
    s2 = df[df['qa.sentinel2'] == 1].copy()
    # remove columns ndvi.landsat, qa.landsat, qa.sentinel2
    s2 = s2.drop(columns=['ndvi.landsat', 'qa.landsat', 'qa.sentinel2'])
    # rename ndvi.sentinel2 to ndvi
    s2 = s2.rename(columns={'ndvi.sentinel2': 'ndvi'})
    # remove any row where ndvi is the same as the previous row
    s2 = s2[s2['ndvi'] != s2['ndvi'].shift(1)]

    # slice off all rows where qa.landsat8 == 1
    l8 = df[df['qa.landsat'] == 1].copy()
    # remove columns ndvi.sentinel2, qa.landsat, qa.sentinel2
    l8 = l8.drop(columns=['ndvi.sentinel2', 'qa.landsat', 'qa.sentinel2'])
    # rename ndvi.landsat to ndvi
    l8 = l8.rename(columns={'ndvi.landsat': 'ndvi'})
    # remove any row where ndvi is the same as the previous row
    l8 = l8[l8['ndvi'] != l8['ndvi'].shift(1)]

    # concat s2 and l8
    m = pd.concat([s2, l8])
    # sort by lat, time
    m = m.sort_values(by=[sort_col, 'time'])
    m = handle_duplicates(m)
    # reset index
    m = m.reset_index(drop=True)
    # in s2, rename column ndvi to ndvi.sentinel2
    s2 = s2.rename(columns={'ndvi': 'ndvi.sentinel2'})
    # in l8, rename column ndvi to ndvi.landsat
    l8 = l8.rename(columns={'ndvi': 'ndvi.landsat'})
    return (m, s2, l8)


def savgol_(m,s2,l8,window_length=20,polyorder=2):
    m = remove_outliers(m)
    # print("After remove_outliers")
    # print(m.head())
    min_date = m["time"].min()
    # print(min_date)
    max_date = m["time"].max()
    # print(max_date)
    date_range = pd.date_range(min_date, max_date, freq='D')
    # print("date_range")
    # print(date_range.shape)

    m1 = pd.DataFrame({"time": date_range})

    # Merge the original 'ndvi' values into the new DataFrame 'm1' using outer join
    m1 = pd.merge(m1, m, on="time", how="left")
    # print("m1")
    # print(m1.shape)
    # print(m1.head(2))
    # print(m1.tail(2))

    # replace any NAN with the value from the previous row
    # print("before ffil")
    # print(m1.tail(30))
    m1 = m1.fillna(method='ffill')
    # if there is a column called "point" coerce it to int
    if "point" in m1.columns:
        m1["point"] = m1["point"].astype(int)
    if "location" in m1.columns:
        m1["location"] = m1["location"].astype(int)
    # print("After merge")
    # print(m1.tail(30))

    # Interpolate missing values in the 'ndvi' column
    m1["ndvi"] = m1["ndvi"].interpolate()

    # Fill missing values with NaN, so the filter doesn't treat them as zeros
    m1["ndvi"] = m1["ndvi"].replace(0, np.nan)

    # Apply the savgol_filter to smooth the "ndvi" column
    m1["ndvi.savgol"] = savgol_filter(m1["ndvi"], window_length, polyorder)

    # drop column ndvi
    m1 = m1.drop(columns=['ndvi'])

    temp = pd.DataFrame({"time": date_range})
    # merge in the original ndvi.sentinel2 values
    temp = pd.merge(temp, s2, on="time", how="left")
    # drop column polygon
    temp = temp.drop(columns=['polygon'])
    # print("temp1")
    # print(temp.tail(20))
    # print(temp.shape)
    # print(m1.shape)
    m1['ndvi.sentinel2'] = temp['ndvi.sentinel2']
    # merge in the original ndvi.landsat values
    temp = pd.DataFrame({"time": date_range})
    temp = pd.merge(temp, l8, on="time", how="left")
    # print("temp l8")
    # print(temp.tail(20))
    m1['ndvi.landsat'] = temp['ndvi.landsat']
    return m1


def savgol(df_,window_length=20,polyorder=2):
    (m,s2,l8) = prepare(df_)
    sort_col = 'point'
    if m.columns[0] != 'point':
        sort_col = 'location'

    for p in m[sort_col].unique():
        m1 = m[m[sort_col]==p]
        s21 = s2[s2[sort_col]==p]
        l81 = l8[l8[sort_col]==p]
        m2 = savgol_(m1,s21,l81,window_length,polyorder)
        if p==0:
            m3 = m2
        else:
            m3 = pd.concat([m3, m2])
    m3.reset_index(drop=True)
    return m3
    