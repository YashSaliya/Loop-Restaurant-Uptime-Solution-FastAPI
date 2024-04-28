from datetime import datetime, timedelta
import pandas as pd
from pytz import timezone
from models import StoreStatus, StoreWorkingHours, StoreTime

def generate_report(current_timestamp):
  """
  Generates a report of store uptime and downtime based on provided data.

  Args:
      current_timestamp (datetime): The current timestamp to consider for report generation.

  Returns:
      pd.DataFrame: A DataFrame containing the report data.
  """

  # Fetch data from database
  store_statuses = session.query(StoreStatus).all()
  store_hours = session.query(StoreWorkingHours).all()
  store_timezones = session.query(StoreTime).all()

  # Convert data to Pandas DataFrames
  df_statuses = pd.DataFrame([(s.store_id, s.timestamp_utc, s.status) for s in store_statuses], columns=['store_id', 'timestamp_utc', 'status'])
  df_hours = pd.DataFrame([(h.store_id, h.day, h.start_time_local, h.end_time_local) for h in store_hours], columns=['store_id', 'dayOfWeek', 'start_time_local', 'end_time_local'])
  df_timezones = pd.DataFrame([(t.store_id, t.timezone_str) for t in store_timezones], columns=['store_id', 'timezone_str'])

  # Merge DataFrames
  df = pd.merge(df_statuses, df_timezones, on='store_id', how='left')
  df = pd.merge(df, df_hours, on='store_id', how='left')

  # Fill missing timezones with default
  df['timezone_str'].fillna('America/Chicago', inplace=True)

  # Define time intervals for report
  last_hour = current_timestamp - timedelta(hours=1)
  last_day = current_timestamp - timedelta(days=1)
  last_week = current_timestamp - timedelta(weeks=1)

  def calculate_uptime_downtime(df_store):
    """
    Calculates uptime and downtime for a single store.

    Args:
        df_store (pd.DataFrame): DataFrame containing data for a single store.

    Returns:
        tuple: A tuple containing uptime and downtime values for the last hour, day, and week.
    """

    # Convert timestamps to local time zone
    df_store['timestamp_local'] = df_store['timestamp_utc'].dt.tz_localize('UTC').dt.tz_convert(df_store['timezone_str'])

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

    return uptime_hour, uptime_day, uptime_week, downtime_hour, downtime_day, downtime_week

  def _calculate_uptime_downtime_interval(df_interval, start, end):
    """
    Helper function to calculate uptime and downtime for a specific interval.
    """

    # Filter data within the interval
    df_interval = df_interval[
      (df_interval['timestamp_local'] >= start) &
      (df_interval['timestamp_local'] <= end)
    ]

    # Calculate total duration of the interval
    total_duration = (end - start).total_seconds() / 60

    # Calculate uptime and downtime based on status changes
    uptime = 0
    downtime = 0
    last_status = None
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


  # Apply calculations for each store
  results = df.groupby('store_id').apply(calculate_uptime_downtime)

  # Create the final report DataFrame
  report_df = pd.DataFrame(results.tolist(), index=results.index, columns=[
    'uptime_last_hour(in minutes)',
    'uptime_last_day(in hours)', 
    'update_last_week(in hours)',
    'downtime_last_hour(in minutes)',
    'downtime_last_day(in hours)',
    'downtime_last_week(in hours)'
  ])

  return report_df