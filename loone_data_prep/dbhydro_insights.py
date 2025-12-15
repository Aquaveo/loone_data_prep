"""
Utilities for interacting with the DBHYDRO Insights database services.

This module provides functions for fetching data from endpoints used
by the South Florida Water Management District's DBHYDRO Insights app.
"""

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
        