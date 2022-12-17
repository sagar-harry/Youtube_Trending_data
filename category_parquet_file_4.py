import argparse
import os
import yaml
from weekly_parquet_file_3 import read_csv_files_combine
from dateutil.relativedelta import relativedelta

def retrieve_params(config):
    with open("params.yaml") as yaml_file:
        params = yaml.safe_load(yaml_file)
    return params

def aggregate_date(params, agg, folder1, folder2, days, months):
    df = read_csv_files_combine(params)
    
    start_date = df["trending_date"].min()
    end_date = df["trending_date"].max()

    current_date1 = start_date
    current_date2 = start_date + relativedelta(days=days, months=months)
    folder_path = params["aggregates"][folder1][folder2]
    while current_date2<=end_date:
        df_final = df.loc[(df["trending_date"]>=current_date1) & (df["trending_date"]<current_date2)]
        catgories = df_final[agg].unique()
        df_final = df_final.groupby(agg)
        for category in catgories:
            if isinstance(category, str):
                file_name = str(category)+"--"+str(current_date1)+"--"+str(current_date2)+ ".gz.parquet"
                dfk = df_final.get_group(category)
                df1 = dfk.groupby("video_id").sum()
                df1.to_parquet(os.path.join(folder_path, file_name), compression="gzip", engine='pyarrow')
        current_date1 = current_date2
        current_date2 += relativedelta(days=days, months=months)
    print("Mission accomplished!")

def generates_aggregates_category(config):
    params = retrieve_params(config=config)
    aggregate_date(params, agg="Category", folder1="category", folder2="trending_date", days=1, months=0)
    aggregate_date(params, agg="region", folder1="region", folder2="trending_date",  days=1, months=0)

    aggregate_date(params, agg="Category", folder1="category", folder2="trending_month",  days=0, months=1)
    aggregate_date(params, agg="region", folder1="region", folder2="trending_month",  days=0, months=1)

    aggregate_date(params, agg="Category", folder1="category", folder2="trending_quarter",  days=0, months=3)
    aggregate_date(params, agg="region", folder1="region", folder2="trending_quarter", days=0, months=3)


if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    generates_aggregates_category(config=parsed_args.config)