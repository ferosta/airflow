import pandas as pd

# file = "d:\\amfedorov\\fedorov\\tmp\\airflow2\\dags\\csvs\\addresses.csv"
file = ".\\dags\\csvs\\addresses.csv"

# read the csv file
results = pd.read_csv(file)
    
# display dataset
print(len(results))
