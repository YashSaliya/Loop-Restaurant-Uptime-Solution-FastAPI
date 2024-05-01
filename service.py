from starlette.responses import JSONResponse
from datetime import datetime, timedelta
import pandas as pd
from pytz import timezone
import pathos.multiprocessing as mp
from utils import parse_timestamp
from models import  engine
import os

reports_processing = set() 


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

  df_statuses = pd.read_sql(query_statuses, con=engine)
  df_hours = pd.read_sql(query_hours, con=engine)
  df_timezones = pd.read_sql(query_timezones, con=engine)
  df_statuses['timestamp_utc'] = df_statuses['timestamp_utc'].apply(lambda x: parse_timestamp(x))
  df_hours['start_time_local'] = df_hours['start_time_local'].apply(lambda x: parse_timestamp(x))
  df_hours['end_time_local'] = df_hours['end_time_local'].apply(lambda x: parse_timestamp(x))
  df = pd.merge(df_statuses, df_timezones, on='store_id', how='left', validate="many_to_many")
  df = pd.merge(df, df_hours, on='store_id', how='left', validate="many_to_many")

  # Assuming default timezones 
  df['timezone_str'].fillna('America/Chicago', inplace=True)


  # Define time intervals for report
  last_hour = current_timestamp - timedelta(hours=1)
  last_day = current_timestamp - timedelta(days=1)
  last_week = current_timestamp - timedelta(weeks=1)

  def calculate_uptime_downtime(store_id,df_store):
    """
    Calculates uptime and downtime for a single store.

    Args:
        store_id : for the store
        df_store (pd.DataFrame): DataFrame containing data for a single store.

    Returns:
        tuple: A tuple containing uptime and downtime values for the last hour, day, and week.
    """

    timezone_string = df_store['timezone_str'].unique()[0]
    df_store['timestamp_local'] = df_store['timestamp_utc'].dt.tz_localize('UTC').dt.tz_convert(timezone_string)

    df_filtered = df_store[
      (df_store['timestamp_local'].dt.dayofweek == df_store['dayOfWeek']) &
      (df_store['timestamp_local'].dt.time >= df_store['start_time_local'].dt.time) &
      (df_store['timestamp_local'].dt.time <= df_store['end_time_local'].dt.time)
    ]

    # Calculate uptime and downtime using interpolation
    uptime_hour, downtime_hour = _calculate_uptime_downtime_interval(df_filtered, last_hour, current_timestamp)
    uptime_day, downtime_day = _calculate_uptime_downtime_interval(df_filtered, last_day, current_timestamp)
    uptime_week, downtime_week = _calculate_uptime_downtime_interval(df_filtered, last_week, current_timestamp)
    return store_id, uptime_hour, uptime_day, uptime_week, downtime_hour, downtime_day, downtime_week


  #Helper function to calculate uptime and downtime for a specific interval.
  def _calculate_uptime_downtime_interval(df_interval, start, end):
    total_duration = (end - start).total_seconds() / 60

    # No observations within business hours, assume downtime for the entire interval
    if df_interval.empty:
        return 0, total_duration 

    tz = df_interval['timestamp_local'].tolist()[0].tzinfo

    start = start.astimezone(tz)
    end = end.astimezone(tz)
    df_interval = df_interval[
      (df_interval['timestamp_local'] >= start) &
      (df_interval['timestamp_local'] <= end)
    ]

    if df_interval.empty:
        return 0, total_duration 

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
  # Use pathos multiprocessing to generate each store id report in parallel 
  with mp.Pool() as pool:
        results = pool.starmap(calculate_uptime_downtime, [(store_id,group) for store_id, group in store_groups])
  
 # Output Dataframe
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


def generate_and_save_report(report_id): 
  """
  Generates the report and saves it to a file.
  """
  # Assuming the timestamp to be max out of the store polling report 
  current_timestamp = datetime.strptime('2023-01-25 18:13:22.47922','%Y-%m-%d %H:%M:%S.%f') 
  
  report_df = generate_report(current_timestamp)
  directory = "reports"

  if not os.path.exists(directory):
    os.makedirs(directory)
  
  report_df.to_csv(f"reports/{report_id}.csv", index=False)
  reports_processing.remove(report_id)