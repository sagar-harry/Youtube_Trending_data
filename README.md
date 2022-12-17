Youtube Trending data
==============================

ETL on youtube trending data

Project Organization
------------

    ├── README.md          <- Documentation of the project
    ├── data_given         <- Excel and Json files given
    │
    ├── processed_data     <- Data generated after adding columns of category,
    │                         net popularit(likes - dislikes), 
    │                         days in top 10 trending,
    │                         months in top trending
    │
    ├── weekly data        <- Parquet files created by combing all excel files from data_processed,
    │                         Combined on weekyl basis
    │
    ├── requirements.txt   <- The requirements file for reproducing the environment
    │
    │               │││ Programs │││
    │
    ├── feature_creation_1.py          <- Source code for adding columns like category,
    │                                     net popularit(likes - dislikes), 
    │                                     days in top 10 trending,
    │                                     months in top trending
    │                                     -> Results saved in data_processed/
    │  
    ├── generate_meta_data_file_2.py   <- Source code for generating standard meta data files
    │  
    ├── validate_meta_2.py             <- Source code for validating new data received
    │  
    ├── weekly_parquet_file_3.py       <- Source code for generating weekly parquet files
    │                                     -> Results saved in weekly_data/ 
    │  
    ├── category_parquet_fle_4.py      <- Source code for genrating aggregate data of category and region
    │                                     -> by date, weekly and quarterly
    │  
    ├── params.yaml                    <- All the params and env variables used in the project
    │                                     
    │  
    ├── standard_csv_meta.txt          <- Standard meta data txt file for validating csv files received 
    │
    └── standard_json_meta.txt         <- Standard meta data txt file for validating json files received


--------

1. Add data files in data_given/ folder
2. Run dvc repro <stage-name> to get output of the stage