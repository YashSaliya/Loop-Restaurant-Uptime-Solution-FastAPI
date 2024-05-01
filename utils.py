from datetime import datetime

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
