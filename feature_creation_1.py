import argparse
import yaml
import os
import pandas as pd
import json
import re

def retrieve_params(config):
    with open("params.yaml") as yaml_file:
        params = yaml.safe_load(yaml_file)
    return params

def retrieve_data_from_json(json_file_path):
    categories_dict = {}
    with open(json_file_path) as json_file:
        content_dict = json.load(json_file)
    items = content_dict["items"]
    for item in items:
        id = int(item["id"])
        category = item["snippet"]["title"]
        categories_dict[id] = category
    return categories_dict

def add_additional_columns(excel_file_path, categories_dict):
    try:
        df = pd.read_csv(excel_file_path, encoding='utf-8')
    except:
        try:
            df = pd.read_csv(excel_file_path, encoding='latin1')
        except:
            try:
                df = pd.read_csv(excel_file_path, encoding='iso-8859-1')
            except:
                try:
                    df = pd.read_csv(excel_file_path, encoding='cp1252')
                except:
                    return None

    df["Category"] = df["category_id"].map(categories_dict)

    df["Net Popularity"] = df["likes"] - df["dislikes"]
    df["Days in top 10 Trending"] = df["video_id"].map(dict(df.groupby("video_id").count()["trending_date"]))
    # Assuming every 7 days will be counted as a week
    df["Weeks in top 10 Trending"] = df["Days in top 10 Trending"].apply(lambda x: x//7)
    
    # Dictionary of all dates each videos was trending- key: video_id; value: list of dates
    dates_list = df.set_index("video_id")["trending_date"].groupby("video_id").apply(lambda x : x.to_numpy().tolist()).to_dict()
    number_of_months = {}
    # Creating combination YYMM for finding number of unique months the video was on trending
    for j,i in zip(dates_list.keys(), dates_list.values()):
        unique_months = [x[:2]+x[-2:] for x in i]
        number_of_months[j] = len(set(unique_months))

    df["Months in top 10 Trending"] = df["video_id"].map(number_of_months)
    return df

def read_and_write_all_excel_files(config):
    params = retrieve_params(config=config)
    files = os.listdir(params["data"]["raw_data"])
    json_files = [i for i in files if "_category_id.json" in i]

    for json_file in json_files:
        excel_file = re.sub("_category_id.json", "videos.csv",json_file, flags=re.IGNORECASE)
        print(excel_file, json_file)
        categories_dict = retrieve_data_from_json(os.path.join(params["data"]["raw_data"], json_file))
        df = add_additional_columns(os.path.join(params["data"]["raw_data"], excel_file), categories_dict)
        df.to_csv(os.path.join(params["data"]["processed_data"], excel_file))
        print("Successful")

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    read_and_write_all_excel_files(parsed_args.config)
    
    