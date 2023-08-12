import sys
from datetime import datetime
from retry import retry
from rpy2.robjects import r
from rpy2.rinterface_lib.embedded import RRuntimeError


DEFAULT_DBKEYS = ["16022", "12509", "12519", "16265", "15611"]
DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(RRuntimeError, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    name: str,
    dbkeys: list = DEFAULT_DBKEYS,
    date_min: str = "1950-01-01",
    date_max: str = DATE_NOW,
    **kwargs: str | list
) -> None:
    dbkeys_str = "\"" + "\", \"".join(dbkeys) + "\""
    r(
        f"""
        # Load the required libraries
        library(rio)
        library(dbhydroR)
        #Stage Data
        {name} = get_hydro(dbkey = c({dbkeys_str}), date_min = "{date_min}", date_max = "{date_max}")
        write.csv({name},file ='{workspace}/{name}.csv')
        """
    )


if __name__ == "__main__":
    args = [sys.argv[1].rstrip("/"), sys.argv[2]]
    if len(sys.argv) >= 4:
        dbkeys = sys.argv[3].strip("[]").replace(" ", "").split(',')
        args.append(dbkeys)
    if len(sys.argv) >= 5:
        date_min = sys.argv[4]
        args.append(date_min)
    if len(sys.argv) >= 6:
        date_max = sys.argv[5]
        args.append(date_max)

    get(*args)
