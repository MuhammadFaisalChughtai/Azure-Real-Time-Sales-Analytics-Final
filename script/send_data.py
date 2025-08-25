import pandas as pd
import json
import time
from azure.eventhub import EventHubProducerClient, EventData
import os

# --- Configuration Section ---
# IMPORTANT: Paste your Event Hubs connection string here.
# It should be inside the quotation marks.
CONNECTION_STR = os.environ.get('EVENT_HUB_CONNECTION_STRING')

EVENT_HUB_NAME = "sales-stream-dt" # This should match the name of your Event Hub instance
CSV_FILE_PATH = "../data/HyperStoreDataSet_Final.csv" # The name of your data file

# --- Load and Prepare the Dataset ---
print(f"Loading data from {CSV_FILE_PATH}...")
# Load the CSV file into a pandas DataFrame.
# The encoding 'latin1' is used to handle special characters in the dataset.
df = pd.read_csv(CSV_FILE_PATH, encoding='latin1')
# Drop rows with missing values to ensure data quality for the simulation.
df = df.dropna()
print("Data loaded and cleaned successfully.")

# --- Create a client to send messages to the Event Hub ---
print("Creating Event Hub producer client...")
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)
print("Client created.")

print("\n--- Starting to send simulated sales data... Press Ctrl+C to stop. ---")
try:
    for index, row in df.iterrows():
        event_data_dict = row.to_dict()
        event_data_str = json.dumps(event_data_dict, default=str)
        event_data_batch = EventData(event_data_str)
        
        with producer:
            producer.send_batch([event_data_batch])
        
        # UPDATED: Print a relevant column from the new data
        print(f"Sent: Product Name {event_data_dict['Product Name']}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\n--- Data streaming stopped by user. ---")
except Exception as e:
    print(f"\n--- An error occurred: {e} ---")