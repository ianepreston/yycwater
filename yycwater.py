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

MEASUREMENT_GROUPS = [
    "Calcium (Ca)(Dissolved)",
    "Chloride (Cl)",
    "Sodium (Na)(Dissolved)",
    "Magnesium (Mg)(Dissolved)",
    "Sulphate (SO4)",
    "Total Alkalinity",
    "pH",
]


def water_pull() -> List[Dict[str, str]]:
    """Get the results from the API."""
    # Hard code this when copied to the NAS
    api_key = os.getenv("APP_TOKEN")
    cutoff_dt = dt.datetime.now() - dt.timedelta(days=365)

    base_url = "https://data.calgary.ca/resource/y8as-bmzj.json"
    columns = [
        "sample_site",
        "sample_date",
        "parameter",
        "numeric_result",
        "result_units",
    ]
    selection = ", ".join(f"{col}" for col in columns)
    quoted_measures = [f"'{item}'" for item in MEASUREMENT_GROUPS]
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


def parse_dates(pull: List[Dict[str, str]]):
    """Turn datetime strings into datetimes for the pull."""
    if isinstance(pull[0]["sample_date"], str):
        for row in pull:
            row["sample_date"] = dt.datetime.strptime(
                row["sample_date"], "%Y-%m-%dT%H:%M:%S.%f"
            )
    return pull


def pivot_pull(pull: List[Dict[str, str]]):
    """Pivot so columns are measures and rows are dates."""
    parsed_pull = parse_dates(pull)
    dates = sorted(list(set(row["sample_date"] for row in parsed_pull)))
    pivot = list()
    for date in dates:
        row = {"sample_date": date}
        observations = [row for row in parsed_pull if row["sample_date"] == date]
        for measure in MEASUREMENT_GROUPS:
            observation = [row for row in observations if row["parameter"] == measure]
            if len(observation) != 1:
                raise ValueError(
                    "Should only have one value per date observation combo."
                )
            row[measure] = observation[0]["numeric_result"]
        pivot.append(row)
    return pivot


def write_out(pull):
    """Save a result to csv.

    Base the filename on the latest available sample.
    Just overwrite if we've already grabbed one. This is likely going to be
    scheduled to run every day, but there won't be updates most days, so we don't want
    to generate a bunch of redundant data. On the data page they note that occasionally
    old samples are revised, and given we don't care about historical state, as long as
    we have the latest data available this is fine.
    """
    pivot = pivot_pull(pull)
    file_name = f"water_data.csv"
    with open(file_name, "w", encoding="utf8", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=pivot[0].keys(),
        )
        writer.writeheader()
        writer.writerows(pivot)


if __name__ == "__main__":
    data = water_pull()
    write_out(data)
