import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import pickle

# NASA API key
API_KEY = "sNjM1Gxthf3AVfl2cSSBw0V0iqoBcM6McEDT6gmg"

# Define NASA API URLs
BASE_URL = "https://api.nasa.gov/DONKI/"
CME_URL = f"{BASE_URL}CME"
GST_URL = f"{BASE_URL}GST"

# Fetch CME data
def fetch_cme_data(start_date, end_date):
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "api_key": API_KEY
    }
    response = requests.get(CME_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch CME data: {response.status_code}")

# Fetch GST data
def fetch_gst_data(start_date, end_date):
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "api_key": API_KEY
    }
    response = requests.get(GST_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch GST data: {response.status_code}")

# Expand linkedEvents into individual rows
def expand_linked_events(data, id_column, time_column, linked_column):
    expanded_rows = []
    for i in data.index:
        activity_id = data.loc[i, id_column]
        start_time = data.loc[i, time_column]
        linked_events = data.loc[i, linked_column]
        if isinstance(linked_events, list):
            for event in linked_events:
                expanded_rows.append({
                    id_column: activity_id,
                    time_column: start_time,
                    "linkedEvent": event.get("activityID")
                })
    return pd.DataFrame(expanded_rows)

# Process CME data
def process_cme_data(cme_data):
    cme_df = pd.DataFrame(cme_data)
    cme_df = cme_df[["activityID", "startTime", "linkedEvents"]].dropna(subset=["linkedEvents"])
    cme_df = expand_linked_events(cme_df, "activityID", "startTime", "linkedEvents")
    cme_df.rename(columns={"startTime": "startTime_CME", "activityID": "cmeID", "linkedEvent": "GST_ActivityID"}, inplace=True)
    cme_df["startTime_CME"] = pd.to_datetime(cme_df["startTime_CME"])
    return cme_df[cme_df["GST_ActivityID"].str.contains("GST")]

# Process GST data
def process_gst_data(gst_data):
    gst_df = pd.DataFrame(gst_data)
    gst_df = gst_df[["gstID", "startTime", "linkedEvents"]].dropna(subset=["linkedEvents"])
    gst_df = gst_df.explode("linkedEvents").dropna(subset=["linkedEvents"])
    gst_df["CME_ActivityID"] = gst_df["linkedEvents"].apply(lambda x: x.get("activityID") if isinstance(x, dict) else None)
    gst_df.rename(columns={"startTime": "startTime_GST"}, inplace=True)
    gst_df["startTime_GST"] = pd.to_datetime(gst_df["startTime_GST"])
    gst_df = gst_df[~gst_df["CME_ActivityID"].isnull()]
    return gst_df[gst_df["CME_ActivityID"].str.contains("CME")]

# Merge CME and GST data
def merge_cme_gst_data(cme_df, gst_df):
    merged_df = pd.merge(
        cme_df,
        gst_df,
        left_on="GST_ActivityID",
        right_on="gstID",
        how="inner"
    )
    merged_df["timeDiff"] = (merged_df["startTime_GST"] - merged_df["startTime_CME"]).dt.total_seconds() / 3600
    return merged_df

# Load from pickle
def load_from_pickle(file_name):
    if os.path.exists(file_name):
        with open(file_name, "rb") as file:
            return pickle.load(file)
    return None

# Save to pickle
def save_to_pickle(data, file_name):
    with open(file_name, "wb") as file:
        pickle.dump(data, file)

# Main execution
if __name__ == "__main__":
    try:
        # Define date range
        START_DATE = "2013-05-01"
        END_DATE = "2024-05-01"

        # File names for pickled data
        CME_PICKLE = "cme_data.pkl"
        GST_PICKLE = "gst_data.pkl"
        MERGED_PICKLE = "merged_data.pkl"

        # Load pickled data if available
        cme_df = load_from_pickle(CME_PICKLE)
        gst_df = load_from_pickle(GST_PICKLE)
        merged_df = load_from_pickle(MERGED_PICKLE)

        if cme_df is None or gst_df is None:
            # Fetch and process data if not available in pickle
            cme_data = fetch_cme_data(START_DATE, END_DATE)
            gst_data = fetch_gst_data(START_DATE, END_DATE)

            cme_df = process_cme_data(cme_data)
            gst_df = process_gst_data(gst_data)

            # Save processed data to pickle files
            save_to_pickle(cme_df, CME_PICKLE)
            save_to_pickle(gst_df, GST_PICKLE)

        if merged_df is None:
            # Merge datasets if not available in pickle
            merged_df = merge_cme_gst_data(cme_df, gst_df)

            # Save merged data to pickle
            save_to_pickle(merged_df, MERGED_PICKLE)

        # Calculate and display average time
        average_time = merged_df["timeDiff"].mean()
        median_time = merged_df["timeDiff"].median()
        print(f"The average time from CME to GST is {average_time:.2f} hours.")
        print(f"The median time from CME to GST is {median_time:.2f} hours.")

        # Export merged data
        merged_df.to_csv("merged_cme_gst_data.csv", index=False)
        print("Data exported to 'merged_cme_gst_data.csv'.")

    except Exception as e:
        print(f"An error occurred: {e}")
