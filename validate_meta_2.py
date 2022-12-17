import argparse
import yaml
import pandas as pd
import io
import json

def retrieve_params(config):
    with open(config) as yaml_file:
        params = yaml.safe_load(yaml_file)
    return params

def validate_csv(params):
    excel_file_path = params["data_new"]["csv_file_path"]

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

    df.columns = [i.strip() for i in df.columns]
    buffer = io.StringIO()
    df.info(buf=buffer, verbose = True, null_counts = False, memory_usage=False)
    s = buffer.getvalue()
    s = s.split("\n")
    del(s[1])
    standard = "\n".join(s)

    with open(params["data"]["standard_csv_meta"]) as file:
        c = file.read()

    return c.strip() == standard.strip()


def validate_json(params):
    json_file_path = params["data_new"]["json_file_path"]

    with open(json_file_path) as json_file:
        standard_dict = json.load(json_file)
    
    standard_json_meta = params["data"]["standard_json_meta"]
    keys1 = standard_dict.keys()
    for i in standard_dict["items"]:
        keys2 = i.keys()
        break
    b = standard_dict["items"][0]["snippet"]
    keys3 = b.keys()
    final_string = str(list(keys1))+ "\n"+ str(list(keys2))+ "\n"+ str(list(keys3))

    with open(standard_json_meta, "r") as meta:
        standard = meta.read()
    
    return final_string == standard

def validate(config):
    params = retrieve_params(config)
    print("CSV files valid: ", "Success" if validate_csv(params) else "Failed")
    print("JSON files valid: ", "Success" if validate_json(params) else "Failed")


if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    validate(parsed_args.config)