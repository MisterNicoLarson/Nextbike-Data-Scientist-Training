import logging

import requests


CITY_API_URL = "https://maps.nextbike.net/maps/nextbike-live.json?list_cities=1"
LIVE_API_URL = "https://maps.nextbike.net/maps/nextbike-live.json"

"""
    Download the latest Nextbike data and return it as a dictionary.

    Returns:
        dict: Live data enriched with country-specific information.

    Raises:
        requests.exceptions.RequestException: If the API request fails.
"""
def extract_json_from_api_nextbike():
    logger = logging.getLogger(__name__)

    try:
        city_response = requests.get(CITY_API_URL, timeout=30)
        city_response.raise_for_status()

        live_response = requests.get(LIVE_API_URL, timeout=30)
        live_response.raise_for_status()

    except requests.exceptions.RequestException:
        logger.exception("Unable to fetch data from Nextbike API")
        raise

    city_data = city_response.json()
    live_data = live_response.json()

    city_infos = {}

    for country in city_data.get("countries", []):
        country_name = country.get("country_name")
        country_code = country.get("country")

        for city in country.get("cities", []):
            uid = city.get("uid")
            if uid is not None:
                city_infos[uid] = {
                    "country": country_name,
                    "country_code": country_code,
                }

    # Injection des informations de pays dans les données live
    for country in live_data.get("countries", []):
        for city in country.get("cities", []):
            uid = city.get("uid")

            if uid in city_infos:
                city["country_name"] = city_infos[uid]["country"]
                city["country"] = city_infos[uid]["country_code"]

    return live_data