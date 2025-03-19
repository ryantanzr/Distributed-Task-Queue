from celery import Celery, chain, group
from celery.schedules import crontab
from celery.signals import task_success, task_failure, beat_init
from requests_sse import EventSource, InvalidStatusCodeError, InvalidContentTypeError
import logging, requests, json, logging
import pandas as pd
import os

# Create a Celery instance
app = Celery('tasks', broker='redis://redis:6379/0', backend='redis://redis:6379/0')

app.conf.beat_schedule = {

    'extract-every-1-minutes': {
        'task': 'tasks.chained_tasks',
        'schedule': crontab(minute='*/1'),
        'args': (200,)
    },

    'extract-every-5-seconds': {
        'task': 'tasks.chained_tasks',
        'schedule': 5.0,
        'args': (100,)
    }

}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Extract 5 events from GitHub Firehose, bind task for retries
# and apply backoff (exponential) to prevent overloading the server 
@app.task(bind=True, max_retries=3, retry_backoff=True)
def extract_data(self, max_events=5):
    logging.info("Extracting data from GitHub Firehose")
    event_count = 0
    events = []
    with EventSource("http://github-firehose.libraries.io/events", timeout=30) as event_source:
        try:
            for event in event_source:
                value = json.loads(event.data)
                size = event.data.__sizeof__()
                events.append(value)
                #logging.info("Received packet of size: %s", size)
                #logging.info("Received packet: %s", pformat(value))
                event_count += 1
                if event_count >= max_events:
                    break
        except InvalidStatusCodeError:
            pass
        except InvalidContentTypeError:
            pass
        except requests.RequestException:
            pass
    
    raw_df = pd.DataFrame(events)
    actor_df = pd.json_normalize(raw_df['actor'])
    payload_df = pd.json_normalize(raw_df['payload'])

    extracted_df = pd.DataFrame()
    extracted_df['id'] = raw_df['id']
    extracted_df['action_type'] = raw_df['type']
    extracted_df['public'] = raw_df['public']
    extracted_df['created_at'] = raw_df['created_at']
    extracted_df['actor_id'] = actor_df['id']
    extracted_df['actor_name'] = actor_df['display_login']
    extracted_df['repo_id'] = payload_df['repository_id']
    extracted_df['push_description'] = payload_df['description'] if 'description' in payload_df.columns else None 
    extracted_df['size'] = payload_df['size']

    logging.info("extraction complete")

    return extracted_df.to_dict(orient='records')

# Conduct some standardization (Getting only the required columns)
@app.task
def standardize_data(df : dict):
    logging.info("Standardizing data")

    df = pd.DataFrame(df)

    # Remove the word 'Event' from the action_type with a regex
    df['action_type'] = df['action_type'].replace('Event', '', regex=True)

    # Convert the created_at column to a dateime object
    # then to a ANSI-compliant string
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Convert the public column to boolean
    df['public'] = df['public'].astype(bool)

    # Convert the repo_id column to integer
    df['repo_id'] = pd.to_numeric(df['repo_id'], errors='coerce').fillna(0)
    df['repo_id'] = df['repo_id'].astype(int)

    # Replace NaN values in the size column with 0
    df['size'] = df['size'].fillna(0)

    # Save the standardized data to a CSV file with the task ID
    file_name = str(standardize_data.request.id) + '.csv'
    file_path = os.path.join(os.getcwd(), "data/csv" , file_name)
    df.to_csv(file_path, index=False)
    logging.info("Data has been standardized and saved to 'standardized_data.csv'")
    
    return df.to_dict(orient='records')

# This task saves the standardized data to a Parquet file
# to be called by a chord
@app.task
def save_to_parquet(df : dict):
    logging.info("Saving data to Parquet")
    file_name = str(save_to_parquet.request.id) + '.parquet'
    file_path = os.path.join(os.getcwd(), "data/parquet" , file_name)
    df = pd.DataFrame(df)
    print(df)
    df.to_parquet(file_path, engine='pyarrow')
    logging.info("Data has been saved to Parquet")

@beat_init.connect
def beat_init_handler(sender=None, **kwargs):
    logging.info("Beat has been initialized")

@task_success.connect(sender=standardize_data)
def standardization_success_handler(sender=None, result=None, **kwargs):
    logging.info("Standardization completed successfully")

@task_failure.connect(sender=standardize_data)
def standardization_failure_handler(sender=None, exception=None, traceback=None, **kwargs):
    logging.error("Standardization failed")

# Below is an example of 2 key primitives in Celery:
# - Chaining: This is where tasks are executed in a sequence
# - Grouping: This is where tasks are executed in parallel
# - Chords: A combination of a group and a callback on success (Not needed in this case)

# Chaining the tasks with celery
@app.task
def chained_tasks(max_events=5):
    return chain(extract_data.s(max_events), standardize_data.s(), save_to_parquet.s())()

# Grouping the tasks with celery, in this case,
# We do multiple micro-batch extractions in parallel
@app.task
def grouped_tasks():
    return group(chained_tasks.s(50)(), chained_tasks.s(100)(), chained_tasks.s(200)())