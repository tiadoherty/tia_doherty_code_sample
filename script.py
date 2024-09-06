#imports: to run this script, spark will need to be installed. I am using the pyspark library to convert the json to RDD and perform transformations. this could also be done with pandas but I use the apache suite at my current job because of the huge data volumes
import json
import requests
from datetime import datetime, timedelta, date
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

#this schema defines the structure of the data that will be pulled from the API
from schema import case_schema

# Create a sample SparkSession
spark = SparkSession.builder \
    .appName("Example App") \
    .getOrCreate()

#this helper function pulls from the API - since it requires internal authentication methods this is a placeholder, and instead I will open a json file that contains deidentified data that is similar to the data that would be pulled from the API
#this function contains conditional logic to retry the API call if it fails
def fetch_page(page_number, headers, num_retries, tickets, start_date, end_date, url_base):
    print(f"Fetch page called with:\npage_number: {page_number}\nheaders: {headers}\nnum_retries: {num_retries}\ntickets (showing len): {len(tickets)}\nstart_date: {start_date}\nend_date: {end_date}")
    ecm_url = f"{url_base}?pageNumber={page_number}&endDate={end_date}&startDate={start_date}"
    print(f"Ecm url used in fetch: {ecm_url}\n")

    response = requests.get(ecm_url, headers=headers)

    if response.status_code == 200:
        print("Response status code is 200\n")
        # On a successful fetch set num retries back to 0
        num_retries = 0

        response_json = response.json()
        current_page_tickets = response_json['result']['tickets']

        tickets.extend(current_page_tickets)
        next_record = response_json['nextRecord']

        #this handles pagination - if there are more records to fetch based on API response, it will call the fetch_page function again with the next page number
        if next_record:
            fetch_page(page_number + 1, headers,
                            num_retries, tickets, start_date, end_date, url_base)
    else:
        print(f"Response failed with status code: {response.status_code}\n")
        # If a fetch fails, try to fetch it two more times to account for intermittent connection issues before failing
        if num_retries <= 2:
            num_retries += 1
            fetch_page(page_number, headers, num_retries,
                            tickets, start_date, end_date, url_base)
        else:
            raise Exception("Api fetch failed after three retries.")

#here I am opening the json file with sample dataset for the purposes of this exercise and adding them into the tickets list
tickets = []

with open('config.json', 'r') as config_file:
    config_data = json.load(config_file)

for ticket in config_data['result']['tickets']:
    tickets.append(ticket)

total_records = config_data['totalRows']

#loading in the job configuration variables; the framework I use to run these scripts requires that the start and end dates be a timestamp but the dates needed to be trimmed to just the date portion to use as Spark's partition column
with open('config.json', 'r') as job_config_file:
    doc = json.load(job_config_file)

start_date = doc["incremental_load_start_ts"][:10]
#check start date and raise exception if the date equals today or greater so that the job only runs if loading data from yesterday or earlier
today = date.today()
start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
if start_date_obj >= today:
    raise Exception("Start date is today or greater than today. Please check the date in the job configuration.")

end_ts = doc["incremental_load_end_ts"]
end_date_obj = datetime.strptime(end_ts, "%Y-%m-%d %H:%M:%S")
#need to subtract one day from the end date to get the correct date range based on API requirements 
new_end_date = end_date_obj - timedelta(days=1)
# Format new_end_date without timestamp
end_date = new_end_date.strftime("%Y-%m-%d")

print(f"Start date: {start_date}\nEnd date: {end_date}")

#split string into list - this is an example of converting between data types in python. I had to create this because I could not store a nested list as a value in the json config file due to the internal framework I was using
def string_to_list(input_string):
    parts = input_string.split(',')
    return parts

args = doc["args"]
print(f'arguments from job config file: {args}')

#once the data has been collected from the API and stored in the tickets list, it is converted to a spark dataframe


