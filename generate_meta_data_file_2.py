import pandas as pd
import argparse
import yaml
import os
import io
import json

def retrieve_params(config):
    with open(config) as yaml_file:
        params= yaml.safe_load(yaml_file)
    return params


def generate_metadata_csv(params):
    excel_file_path = params["data"]["standard_excel_file"]

    df = pd.read_csv(excel_file_path)
    df.columns = [i.strip() for i in df.columns]
    buffer = io.StringIO()
    df.info(buf=buffer, verbose = True, null_counts = False, memory_usage=False)
    s = buffer.getvalue()
    s = s.split("\n")
    del(s[1])
    s = "\n".join(s)
    standard_csv_meta = params["data"]["standard_csv_meta"]
    with open(standard_csv_meta, "a+") as f:
        f.write(s)
        f.write("\n")

def generate_metadata_json(params):
    json_file_path = params["data"]["standard_json_file"]

    with open(json_file_path) as json_file:
        standard_dict = json.load(json_file)
    
    standard_json_meta = params["data"]["standard_json_meta"]
    keys1 = standard_dict.keys()
    for i in standard_dict["items"]:
        keys2 = i.keys()
        break
    b = standard_dict["items"][0]["snippet"]
    keys3 = b.keys()

    with open(standard_json_meta, "w") as meta:
        meta.write(str(list(keys1)))
        meta.write("\n")
        meta.write(str(list(keys2)))
        meta.write("\n")
        meta.write(str(list(keys3)))

def generate_metadata(config):
    params = retrieve_params(config=config)
    generate_metadata_csv(params)
    generate_metadata_json(params)


if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    generate_metadata(parsed_args.config)