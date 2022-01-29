# UW SELA FY21


==============================

For ticket #9559 

### Prerequisites

```
python 3.7
```


### Installing

Create virtual environment for package management
```
python3 -m venv venv
source venv/bin/activate
```

Install required python packages
```
pip install -r requirements.txt
```

After installing the required packages, if you resave the requirements.txt file it will store which version of the CT_Snippets package being used. 
```
pip freeze > requirements.txt
```

Copy the example .env file
```
cp .envexample .env
```

Make sure you update the following values in the newly created .env
```
SF_TOKEN=<your salesforce token>
SF_USERNAME=<your salesforce username>
SF_PASS=<your salesforce password>
```

By default, all .ipynb files are synced to github as .md files using Jupytext. This allows easier version control and also reduces the file size and sensitive data on Github. To convert all md files back into .ipynb run the following command:

```
jupytext --set-formats ipynb,md *.md 

```

Generally, to run a script, make sure you are running it from the top top level directory, for example to run the data prep script:
```
python src/1_data_prep.py 
```



## Project Organization
------------

    ├── LICENSE
    ├── README.md           <- The top-level README for developers using this project.
    ├── data
    │   ├── interim         <- Intermediate data that has been transformed.
    │   ├── processed       <- The final, canonical data sets for modeling.
    │   └── raw             <- The original, immutable data dump.
    │
    ├── reports             <- Generated analysis as HTML, PDF, LaTeX, etc.
    ├── src                 <- Contains all python code
    │   ├── 1_data_prep.py  <- Initial script to load and prep data
    │   ├── 2_analysis.py   <- If needed, script to run analysis or run more complicated processes
    │   └── helpers.py      <- Contains helper functions that are used in main scripts
    │   └── variables.py    <- For any set variables that will be used - empty by default
    │   └── soql_queries.py <- To store any needed SOQL querires
    │
    ├── requirements.txt    <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    ├── notebooks           <- any Jupyter notebooks used for developing ideas

    
--------
<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
