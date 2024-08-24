# MAST30034 Project 1 README.md
- Name: Ambrose He
- Student ID: 1172346

**Research Goal:** My research goal is to analyze how weather conditions from July to December 2023 affect taxi driver earnings in New York City, particularly using machine learning models to identify the most impactful factors.

**Timeline:** The timeline for the research area is July to December 2023.

Before executing any files, make sure you have:

1. Downloaded the Zone files (https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip) and placed the taxi_zone folder in the data/raw/ directory.

To run the pipeline, please visit the `scripts` directory and run the files in order:

2. `tlc_data_collect.ipynb`: This downloads the raw TLC data into the `data/raw` directory.
3. `tlc_preprocess.ipynb`: This notebook details all preprocessing steps and outputs for TLC data from the `data/raw/tlc_data` and `data/curated` directory.
4. `weather_data_collect.ipynb`: This downloads the raw weather data into the `data/raw` directory.
5. `weather_preprocess.ipynb`: This notebook details all preprocessing steps and outputs for weather data from the `data/raw/weather_data` and `data/curated` directory.
6. `dataset_merge.ipynb`: This notebook is used to merge the weather data and TLC data for analysis.
7. `data_model_split.ipynb`: The script is used to find the most impactful features and to split the dataset into training, testing and validation.
8. `baseline_model.ipynb`, `linear_regression.ipynb` and `random_forest.ipynb`, : These notebooks are used to train the models and reveal results.
9. `evaluation_mapping.ipynb`: The script is used to reveal more information about the models in relation to the mappign of NYC.
