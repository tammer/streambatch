import pandas as pd
import numpy as np
from scipy.signal import savgol_filter

#
# Find outliers
# ndvi(t) is an outlier if:
# 1) abs(zscore) > 1.5 (relative to it's neighbours on each side)
# 2) it is "far" away from the mean in absolute terms too, i.e.
#    ndvi(t) - mean > 0.08
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
    m2['outlier'] = (m2['delta'].abs() > 0.08) & (m2['zscore'].abs() > 1.5)
    return m2

# remove_outliers()
# m is a dataframe with columns time and ndvi
# returns a dataframe with outliers removed
def remove_outliers(m):
    m1 = m.copy()
    m1 = find_outliers(m1)
    m2 = m1[m1['outlier']==False]
    # print(f"removed {m1.shape[0] - m2.shape[0]} outliers")
    m2 = m2.drop(columns=['rolling mean', 'rolling std', 'zscore', 'delta', 'outlier'])
    return m2


# handle_duplicates()
# sometimes we end up with observations from s2 and l8 on the same day.
# when this happens, we need reduce to one observation per day
# !!! a better solution is the choose the "better" observation based on before and after
def handle_duplicates(m):
    # find successive rows with the same time
    # if there are two rows with the same time, keep the first one
    m1 = m.copy()
    m1['duplicate'] = m1['time'].eq(m1['time'].shift(-1))
    m2 = m1[m1['duplicate']==False]
    # print(f"removed {m1.shape[0] - m2.shape[0]} duplicates")
    m2 = m2.drop(columns=['duplicate'])
    return m2

# prepare()
# m is a dataframe with columns time, ndvi.sentinel2, ndvi.landsat, qa.sentinel2, qa.landsat
# (basically the output from the server if you ask for sentinel2 and landsat data)
# We convert this into a sparse dataframe with columns time, ndvi
# where there is only an entry if we have a valid ndvi value for that date from at least one source
# we return (m, s2, l8), where m is the sparse dataframe, s2 is the original sentinel2 data, l8 is the original landsat data
def prepare(df):
    # is this points or polygons?
    sort_col = 'point'
    if df.columns[0] != 'point':
        sort_col = 'location'
    df = df.sort_values(by=[sort_col, 'time'])
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
    min_date = m["time"].min()
    max_date = m["time"].max()
    date_range = pd.date_range(min_date, max_date, freq='D')

    # Create a new DataFrame 'm1' with a column 'time' that contains all dates in the range
    m1 = pd.DataFrame({"time": date_range})

    # Populate it with NDVI values where we have them and NaN where we don't
    m1 = pd.merge(m1, m, on="time", how="left")

    # Interpolate missing values in the 'ndvi' column
    m1["ndvi"] = m1["ndvi"].interpolate()

    m1 = m1.ffill() # forward fill the other columns like lat, lon, point, location, etc

    # if there is a column called "point" or "location" coerce it to int
    if "point" in m1.columns:
        m1["point"] = m1["point"].astype(int)
    if "location" in m1.columns:
        m1["location"] = m1["location"].astype(int)


    # Fill missing values with NaN, so the filter doesn't treat them as zeros
    # !!! there shouldnt be any missing values at this point. not sure I need this
    m1["ndvi"] = m1["ndvi"].replace(0, np.nan)

    # Apply the savgol_filter to smooth the "ndvi" column
    m1["ndvi.savgol"] = savgol_filter(m1["ndvi"], window_length, polyorder)

    # rename column ndvi to ndvi.interpolated
    m1 = m1.rename(columns={'ndvi': 'ndvi.interpolated'})

    # now let's add two more columns with the original sentinel2 and landsat values
    # First S2
    temp = pd.DataFrame({"time": date_range})
    # Populate it with NDVI values where we have them and NaN where we don't
    temp = pd.merge(temp, s2, on="time", how="left")
    # add it as a column to m1
    m1['ndvi.sentinel2'] = temp['ndvi.sentinel2']

    # and now Landsat
    temp = pd.DataFrame({"time": date_range})
    temp = pd.merge(temp, l8, on="time", how="left")
    m1['ndvi.landsat'] = temp['ndvi.landsat']

    return m1

# savgol()
# df_ is a dataframe with columns time, ndvi.sentinel2, ndvi.landsat, qa.sentinel2, qa.landsat
# (basically the output from the server if you ask for sentinel2 and landsat data)
# Returns a dataframe with columns time, ndvi, ndvi.sentinel2, ndvi.landsat
# where ndvi is the smoothed savgol ndvi
def savgol(df_,window_length=20,polyorder=2):
    # take the raw data that has columns for both sentinel2 and landsat
    # and convert it into a sparse dataframe with columns time, ndvi (merging the two ndvi columns)
    (m,s2,l8) = prepare(df_)
    
    # is this points or polygons?
    sort_col = 'point'
    if m.columns[0] != 'point':
        sort_col = 'location'

    # process each point or location separately
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
    