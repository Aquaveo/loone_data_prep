import sys
from retry import retry
from rpy2.robjects import r
from rpy2.rinterface_lib.embedded import RRuntimeError


@retry(RRuntimeError, tries=5, delay=15, max_delay=60, backoff=2)
def get(workspace):
    r(
        f"""
        # Load the required libraries
        library(dbhydroR)

        #S65E_Total
        S65E_total = get_hydro(dbkey = c("91656", "AL760"), date_min = "1972-01-01", date_max = "2023-06-30")
        S65E_total[, -1] <- S65E_total[, -1] * (0.0283168466 * 86400)
        write.csv(S65E_total,file ='{workspace}/S65E_total.csv')
        """
    )


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    get(workspace)
