from fastapi import FastAPI, BackgroundTasks
from starlette.responses import JSONResponse
import uvicorn
from models import StoreStatus, StoreWorkingHours, StoreTime
import uuid
from datetime import datetime, timedelta
import pandas as pd
from pytz import timezone
from models import StoreStatus, StoreWorkingHours, StoreTime, session, engine
import os
import pathos.multiprocessing as mp

app = FastAPI()

reports_processing = set() 
def parse_timestamp(timestamp_str):
    try:
        return datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S.%f')  # Try parsing with milliseconds
    except ValueError:
        try:
            return datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')  # Fallback to parsing without milliseconds
        except ValueError:
            try:
                return datetime.strptime(timestamp_str.strip(), '%H:%M:%S')  # Try parsing without date
            except ValueError:
                return datetime.strptime(timestamp_str.strip(), '%H:%M:%S.%f')  # Fallback to parsing without milliseconds

def generate_report(current_timestamp):
  """
  Generates a report of store uptime and downtime based on provided data.

  Args:
      current_timestamp (datetime): The current timestamp to consider for report generation.

  Returns:
      pd.DataFrame: A DataFrame containing the report data.
  """

  query_statuses = 'SELECT store_id, timestamp_utc, status FROM store_status'
  query_hours = 'SELECT store_id, day as dayOfWeek, start_time_local, end_time_local FROM store_working_hours'
  query_timezones = 'SELECT store_id, timezone_str FROM store_timezone'

  # Use pandas to query the database and store the results in DataFrames
  df_statuses = pd.read_sql(query_statuses, con=engine)
  df_hours = pd.read_sql(query_hours, con=engine)
  df_timezones = pd.read_sql(query_timezones, con=engine)

  df_statuses['timestamp_utc'] = df_statuses['timestamp_utc'].apply(lambda x: parse_timestamp(x))
  df_hours['start_time_local'] = df_hours['start_time_local'].apply(lambda x: parse_timestamp(x))
  df_hours['end_time_local'] = df_hours['end_time_local'].apply(lambda x: parse_timestamp(x))


  # Merge DataFrames
  df = pd.merge(df_statuses, df_timezones, on='store_id', how='left', validate="many_to_many")
  df = pd.merge(df, df_hours, on='store_id', how='left', validate="many_to_many")

  # Fill missing timezones with default
  df['timezone_str'].fillna('America/Chicago', inplace=True)


  # Define time intervals for report
  last_hour = current_timestamp - timedelta(hours=1)
  last_day = current_timestamp - timedelta(days=1)
  last_week = current_timestamp - timedelta(weeks=1)

  def calculate_uptime_downtime(df_store,_):
    """
    Calculates uptime and downtime for a single store.

    Args:
        df_store (pd.DataFrame): DataFrame containing data for a single store.

    Returns:
        tuple: A tuple containing uptime and downtime values for the last hour, day, and week.
    """

    # Convert timestamps to local time zone
    timezone_string = df_store['timezone_str'].unique()[0]
    df_store['timestamp_local'] = df_store['timestamp_utc'].dt.tz_localize('UTC').dt.tz_convert(timezone_string)

    # Filter data within business hours and time intervals
    df_filtered = df_store[
      (df_store['timestamp_local'].dt.dayofweek == df_store['dayOfWeek']) &
      (df_store['timestamp_local'].dt.time >= df_store['start_time_local'].dt.time) &
      (df_store['timestamp_local'].dt.time <= df_store['end_time_local'].dt.time)
    ]

    # Calculate uptime and downtime using interpolation
    uptime_hour, downtime_hour = _calculate_uptime_downtime_interval(df_filtered, last_hour, current_timestamp)
    uptime_day, downtime_day = _calculate_uptime_downtime_interval(df_filtered, last_day, current_timestamp)
    uptime_week, downtime_week = _calculate_uptime_downtime_interval(df_filtered, last_week, current_timestamp)
    store_id = df_store['store_id'].unique().tolist()[0]
    return store_id, uptime_hour, uptime_day, uptime_week, downtime_hour, downtime_day, downtime_week

  def _calculate_uptime_downtime_interval(df_interval, start, end):
    """
    Helper function to calculate uptime and downtime for a specific interval.
    """
    # Calculate total duration of the interval
    total_duration = (end - start).total_seconds() / 60

    if df_interval.empty:
        # No observations within business hours, assume downtime for the entire interval
        return 0, total_duration 

    # Convert 'start' and 'end' to datetime64 objects
    tz = df_interval['timestamp_local'].tolist()[0].tzinfo

    start = start.astimezone(tz)
    end = end.astimezone(tz)
    # Filter data within the interval
    df_interval = df_interval[
      (df_interval['timestamp_local'] >= start) &
      (df_interval['timestamp_local'] <= end)
    ]

    if df_interval.empty:
        # No observations within business hours, assume downtime for the entire interval
        return 0, total_duration 

    # Calculate uptime and downtime based on status changes
    uptime = 0
    downtime = 0
    last_status = None
    last_timestamp = ''
    for _, row in df_interval.iterrows():
      if last_status is None:
        last_status = row['status']
      elif row['status'] != last_status:
        duration = (row['timestamp_local'] - last_timestamp).total_seconds() / 60
        if last_status == 'active':
          uptime += duration 
        else: 
          downtime += duration
        last_status = row['status'] 
      last_timestamp = row['timestamp_local'] 
    
    # Handle the last interval
    if last_status == 'active':
       uptime += (end - last_timestamp).total_seconds() / 60 
    else: 
       downtime += (end - last_timestamp).total_seconds() / 60 

    return uptime, downtime
  store_groups = df.groupby('store_id')
  with mp.Pool() as pool:
        # Apply calculations in parallel
        results = pool.starmap(calculate_uptime_downtime, [(group, current_timestamp) for _, group in store_groups])
  
 # Create the final report DataFrame
  report_df = pd.DataFrame(results, columns=[
        'store_id',
        'uptime_last_hour(in minutes)',
        'uptime_last_day(in hours)', 
        'update_last_week(in hours)',
        'downtime_last_hour(in minutes)',
        'downtime_last_day(in hours)',
        'downtime_last_week(in hours)'
    ])

  return report_df

@app.get("/")
def home():
    return {"message": "First FastAPI app"}

@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
  """
  Triggers the report generation process in the background.
  """
  report_id = str(uuid.uuid4())
  reports_processing.add(report_id)
  background_tasks.add_task(generate_and_save_report, report_id)
  return JSONResponse({"report_id": report_id})

def generate_and_save_report(report_id): 
  """
  Generates the report and saves it to a file.
  """
  # Get the current timestamp (replace with actual logic if needed)
  current_timestamp = datetime.strptime('2023-01-25 18:13:22.47922','%Y-%m-%d %H:%M:%S.%f') 
  
  # Generate the report
  report_df = generate_report(current_timestamp)
  directory = "reports"

    # Check if the directory exists, and if not, create it
  if not os.path.exists(directory):
    os.makedirs(directory)

  
  # Save the report as a CSV file (replace with your preferred storage method)
  report_df.to_csv(f"reports/{report_id}.csv", index=False)
  reports_processing.remove(report_id)

@app.get("/get_report")
async def get_report(report_id: str):
  """
  Retrieves the status or the generated report.
  """
  report_path = f"reports/{report_id}.csv"
  if os.path.exists(report_path):
    with open(report_path, 'r') as f:
      report_data = f.read()
    return JSONResponse({"status": "Complete", "report": report_data})
  elif report_id in reports_processing:
    return "Running"
  else:
    return "Not found"

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
