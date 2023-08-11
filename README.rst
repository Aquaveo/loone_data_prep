LOONE_DATA_PREP
---------------

Prepare data for the LOONE water quality model.

Link to LOONE model repository: https://github.com/osamatarabih/LOONE

**Prerequisites:**

* R (https://www.r-project.org/)
* R packages: dbhydroR, rio, dplyr

**Installation:**

::

    cd /path/to/loone_data_prep/
    pip install .

**Examples**

*From the command line:*

::

    # Get flow data
    python -m loone_data_prep.flow_data.get_inflows /path/to/workspace/
    python -m loone_data_prep.flow_data.get_outflows /path/to/workspace/
    python -m loone_data_prep.flow_data.S65E_total /path/to/workspace/

    # Get water quality data
    python -m loone_data_prep.water_quality_data.get_inflows /path/to/workspace/
    python -m loone_data_prep.water_quality_data.get_lake_wq /path/to/workspace/

    # Get weather data
    python -m loone_data_prep.weather_data.get_all /path/to/workspace/

    # Get water level
    python -m loone_data_prep.water_level_data.get_all /path/to/workspace/

    # Interpolate data
    python -m loone_data_prep.utils interp_all /path/to/workspace/

    # Prepare data for LOONE
    python -m loone_data_prep.LOONE_DATA_PREP /path/to/workspace/ /path/to/output/directory/

*From Python:*

::

    from loone_data_prep.utils import get_dbkeys
    from loone_data_prep.water_level_data import hydro
    from loone_data_prep import LOONE_DATA_PREP

    input_dir = '/path/to/workspace/'
    output_dir = '/path/to/output/directory/'

    # Get dbkeys for water level data
    dbkeys = get_dbkeys(
        station_ids=["L001", "L005", "L006", "LZ40"],
        category="SW",
        param="STG",
        stat="MEAN",
        recorder="CR10",
        freq="DA",
        detail_level="dbkey"
    )

    # Get water level data
    hydro.get(
        workspace=input_dir,
        name="lo_stage",
        dbkeys=dbkeys,
        date_min="1950-01-01",
        date_max="2023-03-31"
    )

    # Prepare data for LOONE
    LOONE_DATA_PREP(input_dir, output_dir)
