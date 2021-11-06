"""Get water quality data from the city of Calgary."""
import csv
import datetime as dt
import json
import os
from typing import Dict, List
from urllib.parse import urlencode
from urllib.request import urlopen

from dotenv import load_dotenv

load_dotenv()


def water_pull() -> List[Dict[str, str]]:
    """Get the results from the API."""
    # Hard code this when copied to the NAS
    api_key = os.getenv("APP_TOKEN")
    # Might have to tweak this date range but it works while I'm testing
    # We only want the latest sample but it's unclear how often they
    # Survey this site, so getting the last 30 days seems safe
    cutoff_dt = dt.datetime.now() - dt.timedelta(days=30)

    base_url = "https://data.calgary.ca/resource/y8as-bmzj.json"
    columns = [
        "sample_site",
        "sample_date",
        "parameter",
        "numeric_result",
        "result_units",
    ]
    selection = ", ".join(f"{col}" for col in columns)
    measurement_groups = [
        "Calcium (Ca)(Dissolved)",
        "Chloride (Cl)",
        "Sodium (Na)(Dissolved)",
        "Potassium (K)(Dissolved)",
        "Magnesium (Mg)(Dissolved)",
        "Sulphate (SO4)",
    ]
    quoted_measures = [f"'{item}'" for item in measurement_groups]
    items_in = f"parameter in({', '.join(item for item in quoted_measures)})"
    date_filter = f"sample_date > '{cutoff_dt:%Y-%m-%dT%H:%M:%S}'"
    where_clause = f"{items_in} AND {date_filter}"
    parameters = {
        "$$app_token": api_key,
        "$select": selection,
        "$where": where_clause,
        "site_key": "SUR_ER-SYB",
    }

    query_string = urlencode(parameters)

    full_url = f"{base_url}?{query_string}"
    stdresult = urlopen(full_url)

    raw_data = stdresult.read()
    encoding = stdresult.info().get_content_charset("utf8")  # JSON default
    return json.loads(raw_data.decode(encoding))


def sample_date(pull: List[Dict[str, str]]) -> str:
    """Get the latest sample date from a query.

    Generally we expect to only pull one sample but it's possible
    the 30 day period will cover more than one. We're using this
    to generate a filename, and we want that based on the latest available
    sample.
    """
    dates = [
        dt.datetime.strptime(row["sample_date"], "%Y-%m-%dT%H:%M:%S.%f") for row in pull
    ]
    max_date = max(dates)
    date_string = dt.datetime.strftime(max_date, "%Y-%m-%d")
    return date_string


def write_out(pull):
    """Save a result to csv.

    Base the filename on the latest available sample.
    Just overwrite if we've already grabbed one. This is likely going to be
    scheduled to run every day, but there won't be updates most days, so we don't want
    to generate a bunch of redundant data. On the data page they note that occasionally
    old samples are revised, and given we don't care about historical state, as long as
    we have the latest data available this is fine.
    """
    file_name = f"water_data-{sample_date(pull)}.csv"
    with open(file_name, "w", encoding="utf8", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=pull[0].keys(),
        )
        writer.writeheader()
        writer.writerows(pull)


if __name__ == "__main__":
    data = water_pull()
    write_out(data)
