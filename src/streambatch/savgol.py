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
    # print("here")
    # print(m2.head())
    # remove the last 5 columns of m2
    m2 = m2.drop(columns=['rolling mean', 'rolling std', 'zscore', 'delta', 'outlier'])
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
    # reset index
    m = m.reset_index(drop=True)
    return m


def savgol(df_,window_length=20,polyorder=2):
    m = prepare(df_)
    # print("After prepare")
    # print(m.head())
    # group by time, mean of ndvi (sometimes two observations on the same day)
    # m = m.groupby('time').mean().reset_index() !!! doesnt work anyway, not accoutning for multiple points
    m = remove_outliers(m)
    # print("After remove_outliers")
    # print(m.head())
    min_date = m["time"].min()
    max_date = m["time"].max()
    date_range = pd.date_range(min_date, max_date, freq='D')
    m1 = pd.DataFrame({"time": date_range})

    # Merge the original 'ndvi' values into the new DataFrame 'm1' using outer join
    m1 = pd.merge(m1, m, on="time", how="left")
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

    return m1