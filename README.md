# FDA ETL

This python script gets data from FDA websites and produces several formatted excel files in the "finished data" directory.

## To Setup

Create a virtual environment and install requirements
```
python -m venv virtualenv
virtualenv\scripts\activate
pip install -r requirements.txt
```

## To Run

```
python main.py
```

default output data format is xlsx but you can change the output format to csv with the following args

```
python main.py csv
```

