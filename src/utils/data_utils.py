import re
from datetime import datetime


def parse_log_line(line):
    # regex to match the log line format
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'
    match = re.match(pattern, line)

    if not match:
        raise ValueError(f"Invalid log line format: {line}")

    ip, timestamp, request, status, bytes_sent, referer, user_agent = match.groups()

    # parse further
    request_parts = request.split()
    method = request_parts[0] if len(request_parts) > 0 else ""
    path = request_parts[1] if len(request_parts) > 1 else ""
    protocol = request_parts[2] if len(request_parts) > 2 else ""

    timestamp = datetime.strptime(timestamp, "%d/%b/%Y:%H:%M:%S %z")

    return {
        "ip": ip,
        "timestamp": timestamp,
        "method": method,
        "path": path,
        "protocol": protocol,
        "status": status,
        "bytes_sent": int(bytes_sent),
        "referer": referer,
        "user_agent": user_agent,
    }
