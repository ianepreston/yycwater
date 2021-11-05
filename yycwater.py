"""Get water quality data from the city of Calgary."""
import datetime as dt
import os
from urllib.parse import urlencode

from dotenv import load_dotenv
import requests

load_dotenv()

api_key = os.getenv("API_KEY")
print(api_key)
cutoff_dt = dt.datetime.now() - dt.timedelta(days=5)

base_url = "https://data.calgary.ca/resource/y8as-bmzj.json"


parameters = {
	# "$limit": 200
	"$where": f"sample_date > '{cutoff_dt:%Y-%m-%dT%H:%M:%S}'"
}
query_string = urlencode(parameters)

full_url = f"{base_url}?{query_string}"
# Remove verify=False when you're at home
result = requests.get(full_url, verify=False).json()
print("hurray!")