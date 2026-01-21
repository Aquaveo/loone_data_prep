"""
Utilities for interacting with the DBHYDRO Insights database services.

This module provides functions for fetching data from endpoints used
by the South Florida Water Management District's DBHYDRO Insights app.
"""

from datetime import datetime
import requests


def get_dbhydro_station_metadata(station_id: str) -> dict | None:
    """
    Fetches metadata for a specific station from the DBHYDRO_SiteStation service.
    
    Args:
        station_id (str): The ID of the station for which to fetch metadata. Examples: 'FISHP', 'L OKEE', etc.
    
    Returns:
        dict: A dictionary containing the metadata of the station, or None if the request fails.
    """
    # Build the request URL with the provided station ID
    request_url = 'https://geoweb.sfwmd.gov/agsext2/rest/services/MonitoringLocations/DBHYDRO_SiteStation/MapServer/4/query'
    
    params = {
        'f': 'json',
        'outFields': '*',
        'spatialRel': 'esriSpatialRelIntersects',
        'where': f"(STATION = '{station_id}')"
    }
    
    # Send the GET request to the specified URL with the parameters
    try:
        response = requests.get(request_url, params=params)
    except requests.exceptions.RequestException:
        return None
    
    # Successful Request
    if response.status_code == 200:
        # Parse the JSON response
        json = response.json()
        
        # No data given back for given station ID
        if not json['features']:
            return None
        
        # Data given back, return the JSON response
        return json
    
    # Failure
    return None


def get_dbhydro_continuous_timeseries_metadata(
    station_ids: list[str],
    categories: list[str] | None = ['ALL'],
    parameters: list[str] | None = ['ALL'],
    statistics: list[str] | None = ['ALL'],
    recorders: list[str] | None = ['ALL'],
    frequencies: list[str] | None = ['ALL']
) -> dict | None:
    """Fetches metadata for continuous time series data from the DBHYDRO Insights service.
    
    Args:
        station_ids (list[str]): List of station IDs to query.
        categories (list[str] | None): List of categories to filter by. Defaults to ['ALL'].
        parameters (list[str] | None): List of parameters to filter by. Defaults to ['ALL'].
        statistics (list[str] | None): List of statistics to filter by. Defaults to ['ALL'].
        recorders (list[str] | None): List of recorders to filter by. Defaults to ['ALL'].
        frequencies (list[str] | None): List of frequencies to filter by. Defaults to ['ALL'].

    Returns:
        dict | None: The JSON response from the API if successful, otherwise None.
    
    Raises:
        Exception: If the request fails.
    """
    # Build the request URL
    request_url = 'https://insightsdata.api.sfwmd.gov/v1/insights-data/cont/ts'
    
    # Build the locations list
    locations = []
    
    for station_id in station_ids:
        # Build the location dictionary for this station_id
        location = {
            'name': station_id,
            'type': 'STATION',
        }
        
        # Add location to the locations list
        locations.append(location)
    
    # Build the data payload
    data = {
        'query': {
            'locations': locations,
            'parameters': parameters,
            'category': categories,
            'statistic': statistics,
            'recorder': recorders,
            'frequency': frequencies,
            'dbkeys': ['ALL'],
        }
    }
    
    # Send the POST request to the specified URL with the parameters
    response = requests.post(request_url, json=data)

    # Successful Request
    if response.status_code == 200:
        # Parse the JSON response
        json = response.json()
        
        # No data given back for given station ID
        if not json['results']:
            return None
        
        # Data given back, return the JSON response
        return json
    
    # Failure
    raise Exception(f"Request failed with status code {response.status_code}: {response.text}")


def get_dbhydro_water_quality_metadata(stations: list[str], test_numbers: list[int]) -> dict | None:
    """Fetches metadata for water quality data from the DBHYDRO Insights service.
    
    Args:
        stations (list[str]): List of station names to get water quality metadata for.
        test_numbers (list[int]): List of test numbers to get data for. Test numbers map to parameters. Example: 25 maps to 'PHOSPHATE, TOTAL AS P'.
    
    Returns:
        dict | None: The JSON response from the API if successful, otherwise None.
    
    Raises:
        Exception: If the request fails.
    """
    # Build the request URL
    request_url = 'https://insightsdata.api.sfwmd.gov/v1/insights-data/chem/ts'
    
    # Build the locations list
    locations = []
    
    for station in stations:
        # Build the location dictionary for this station/site
        location = {
            'name': station,
            'type': 'SITE',
        }
        
        # Add location to the locations list
        locations.append(location)
    
    # Build the query parameters
    query_parameters = {
        'offset': 0,
        'limit': 1000,
        'sort': 'project,location,parameterDesc,matrix,method',
        'startDate': '19000101',
        'endDate': datetime.now().strftime("%Y%m%d"),
        'period': '',
    }
    
    # Build the data payload
    payload = {
        'query': {
            'locations': locations,
            'matrices': ['ALL'],
            'methods': ['ALL'],
            'paramGroups': ['ALL'],
            'parameters': [str(num) for num in test_numbers],
            'projects': ['ALL'],
            'sampleTypes': ['ALL'],
        }
    }
    
    # Send the POST request to the specified URL with the parameters
    response = requests.post(request_url, params=query_parameters, json=payload)

    # Successful Request
    if response.status_code == 200:
        # Parse the JSON response
        json = response.json()
        
        # No data given back for given station ID
        if not json['results']:
            return None
        
        # Data given back, return the JSON response
        return json
    
    # Failure
    raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
