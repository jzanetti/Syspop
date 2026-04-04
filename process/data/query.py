from requests import get, exceptions
import xml.etree.ElementTree as ET
from pandas import DataFrame
from pandas import to_numeric


def stats_data_proc(data):
    data["value"] = to_numeric(data["value"], errors="coerce")
    data = data.dropna()
    data["value"] = data["value"].astype(int)
    return data


def obtain_stats_data(api_url: str, api_key: str or None = None):

    if api_key is None:
        raise Exception("No proper Stats API is provided")

    # Set headers with the API key
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    # Make the GET request
    try:
        response = get(api_url, headers=headers)
        response.raise_for_status()  # Check for HTTP errors
    except exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if response.status_code == 401:
            print(
                "Authentication failed. Please check your API key or obtain a valid one from https://api.data.stats.govt.nz/"
            )
        elif response.status_code == 400:
            print(
                "Bad Request. The URL or dimensions may be invalid. Check the API documentation or simplify the query."
            )
        print(f"Raw response: {response.text}")
        exit()
    except exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        exit()

    # Check if response is empty
    if not response.text:
        print("Error: Empty response received from the API.")
        exit()
    xml_text = response.text  # your XML string

    # Parse XML
    root = ET.fromstring(xml_text)

    # Namespaces
    ns = {
        "generic": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
        "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    }

    rows = []
    # Iterate over all Obs elements
    for obs in root.findall(".//generic:Obs", ns):
        row = {}

        # Extract all key/value pairs from ObsKey
        for val in obs.findall("./generic:ObsKey/generic:Value", ns):
            row[val.attrib["id"]] = val.attrib["value"]

        # Extract the observed value
        obs_value = obs.find("./generic:ObsValue", ns)
        if obs_value is not None:
            row["OBS_VALUE"] = obs_value.attrib["value"]

        rows.append(row)

    # Convert to DataFrame
    return DataFrame(rows)
