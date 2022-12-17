import argparse
import yaml
import pandas as pd
import os
import datetime

def retrieve_params(config):
    with open("params.yaml") as yaml_file:
        params = yaml.safe_load(yaml_file)
    return params

def read_csv_files_combine(params):
    files_path = params["data"]["processed_data"]
    excel_files = os.listdir(files_path)
    first_file_name = excel_files.pop(0)
    df = pd.read_csv(os.path.join(files_path, first_file_name))
    df["region"] = first_file_name.strip("videos.csv")
    for i in excel_files:
        df1 = pd.read_csv(os.path.join(files_path, i))
        df1["region"] = i.strip("videos.csv")
        df = pd.concat([df ,df1], axis=0)

    df["trending_date"] = df["trending_date"].map(lambda x:"20"+str(x))
    df["trending_date"] = pd.to_datetime(df["trending_date"], format='%Y.%d.%m').dt.date
    df.reset_index(inplace=True)

    return df

def splitting_based_on_dates(config):
    params = retrieve_params(config=config)
    df = read_csv_files_combine(params)
    
    start_date = df["trending_date"].min()
    end_date = df["trending_date"].max()
    current_date1 = start_date
    current_date2 = start_date + datetime.timedelta(days=7)
    folder_path = params["data_weekly"]["file_path"]
    while current_date2<=end_date:
        file_name = str(current_date1)+"--"+str(current_date2)+".gz.parquet"
        df_final = df.loc[(df["trending_date"]>=current_date1) & (df["trending_date"]<current_date2)]
        df_final.to_parquet(os.path.join(folder_path, file_name), compression="gzip", engine='pyarrow')
        current_date1 = current_date2
        current_date2 += datetime.timedelta(days=7)
    print("Mission accomplished!")
    

if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    splitting_based_on_dates(parsed_args.config)
    
