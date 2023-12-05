# Database Setup Instructions

## Overview
This repository contains scripts and configurations for setting up MongoDB and MySQL databases, along with a script for importing data into these databases.

## Files Description
- `setup_mongo.py`: Script for setting up the MongoDB database.
- `setup_mysql.py`: Script for setting up the MySQL database.
- `import_data.py`: Script for executing all setup scripts.
- `requirements.txt`: Contains a list of Python packages required to run the scripts.

## Prerequisites
- Python 3.x installed.
- Access to MongoDB and MySQL servers.
- Downloaded Yelp dataset.

## Installation
1. Clone the repository to your local machine.
2. Install the required Python packages:
```pip install -r requirements.txt```

## Adding the Yelp Dataset
1. Download the Yelp dataset from the official [Yelp Dataset page](https://www.yelp.com/dataset).
2. Place the downloaded dataset in the `data` folder, which should be in the same directory as the scripts.


## Configuration
Before running the setup scripts, ensure you have the correct server credentials, including the IP address and other necessary details, configured in `setup_mongo.py` and `setup_mysql.py`.

## Running the Scripts
Run the setup scripts to configure the databases:
```python import_data.py```


## Security Note
The setup scripts contain sensitive information such as server credentials. Ensure that these details are secured and not exposed in public repositories or unsecured files.

## Support
For any issues or questions, please open an issue in the repository or contact the repository maintainer.

