import sys
import os
import datetime
import math
from calendar import monthrange
import numpy as np
import pandas as pd
from retry import retry
from scipy.optimize import fsolve
from scipy import interpolate
from rpy2.robjects import r
from rpy2.rinterface_lib.embedded import RRuntimeError


INTERP_DICT = {
    "PHOSPHATE, TOTAL AS P": {
        "units": "mg/L",
        "station_ids": ["S65E", "FECSR78", "CULV10A", "S71", "S72", "S84", "S127", "S133", "S135", "S154", "S191",
                        "S308C", "S4", "L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
    },
    "PHOSPHATE, ORTHO AS P": {
        "units": "mg/L",
        "station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
    },
    "NITRATE+NITRITE-N": {
        "units": "mg/L",
        "station_ids": ["S65E", "FECSR78", "CULV10A", "S71", "S72", "S84", "S127", "S133", "S135", "S154", "S191",
                        "S308C", "S4", "L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
    },
    "AMMONIA-N": {
        "units": "mg/L",
        "station_ids": ["S65E", "FECSR78", "CULV10A", "S71", "S72", "S84", "S127", "S133", "S135", "S154", "S191",
                        "S308C", "S4", "L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
    },
    "CHLOROPHYLL-A(LC)": {
        "units": "ug/L",
        "station_ids": ["S65E", "FECSR78", "CULV10A", "S71", "S72", "S84", "S127", "S133", "S135", "S154", "S191",
                        "S308C", "S4", "L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
    },
    "CHLOROPHYLL-A, CORRECTED": {
        "units": "ug/L",
        "station_ids": ["S65E", "FECSR78", "CULV10A", "S71", "S72", "S84", "S127", "S133", "S135", "S154", "S191",
                        "S308C", "S4", "L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
    },
    "DISSOLVED OXYGEN": {
        "units": "mg/L",
        "station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
    },
    "RADP": {
        "units": "MICROMOLE/m^2/s",
        "station_ids": ["L001", "L005", "L006", "LZ40"]
    },
    "RADT": {
        "units": "kW/m^2",
        "station_ids": ["L001", "L005", "L006", "LZ40"]
    },
}


@retry(RRuntimeError, tries=5, delay=15, max_delay=60, backoff=2)
def get_dbkeys(
    station_ids: list,
    category: str,
    param: str,
    stat: str,
    recorder: str,
    freq: str = "DA",
    detail_level: str = "dbkey",
    *args: str
) -> str:
    station_ids_str = "\"" + "\", \"".join(station_ids) + "\""

    dbkeys = r(
        f"""
        library(dbhydroR)

        station_ids <- c({station_ids_str})
        dbkeys <- get_dbkey(stationid = station_ids,  category = "{category}", param = "{param}", stat = "{stat}", recorder="{recorder}", freq = "{freq}", detail.level = "{detail_level}")
        print(dbkeys)
        return(dbkeys)
        """  # noqa: E501
    )

    return dbkeys


def data_interpolations(
    workspace: str,
    parameter: str = 'RADP',
    units: str = 'MICROMOLE/m^2/s',
    station_ids: list = ["L001", "L005", "L006", "LZ40"],
    *args: str
) -> None:
    """
    Args:
        workspace (str): _description_
        station (list, optional): Defaults to '["L001", "L005", "L006", "LZ40"]'.
        parameter (str, optional): NITRATE+NITRITE-N, AMMONIA-N, PHOSPHATE, TOTAL AS P, PHOSPHATE, ORTHO AS P,
            CHLOROPHYLL-A, CORRECTED, CHLOROPHYLL-A(LC), RADP. Defaults to 'RADP'.
        units (str, optional): mg/L, ug/L, MICROMOLE/m^2/s. Defaults to 'MICROMOLE/m^2/s'.
    """

    for station in station_ids:
        name = f'{station}_{parameter}'
        path = f'{workspace}/{name}.csv'

        if not os.path.exists(path):
            name = f'water_quality_{name}'
            path = f'{workspace}/{name}.csv'
            if not os.path.exists(path):
                print(f'Skipping "{name}" File does not exist.')
                continue

        Data_In = pd.read_csv(path)

        # check there is more than one row in the file for interpolation.
        if len(Data_In.iloc[:]) < 10:
            print(f'"{name}" file does not have enough values to interpolate.')
            continue

        Data_In = Data_In.set_index(['date'])
        Data_In.index = pd.to_datetime(Data_In.index, unit = 'ns')
        Data_df = Data_In.resample('D').mean()
        Data_df = Data_df.dropna(subset = ['%s_%s_%s'%(station, parameter, units)])
        Data_df = Data_df.reset_index()
        Data_df['Yr_M'] = pd.to_datetime(Data_df['date']).dt.to_period('M')
        start_date = Data_df['date'].iloc[0]
        end_date = Data_df['date'].iloc[-1]
        date_rng = pd.date_range(start = start_date, end = end_date, freq = 'M')
        Monthly_df = pd.DataFrame(date_rng, columns=['date'])
        Monthly_df['Yr_M'] = pd.to_datetime(Monthly_df['date']).dt.to_period('M')
        New_date = []
        New_data = []
        Days = []
        Days_cum = []
        #Set index for the two dataframes
        Data_df = Data_df.set_index(['Yr_M'])
        Monthly_df = Monthly_df.set_index(['Yr_M'])
        for i in Monthly_df.index:
            if i in Data_df.index:
                if type(Data_df.loc[i]['date']) == pd.Timestamp:
                    New_date.append(Data_df.loc[i]['date'])
                    New_data.append(Data_df.loc[i]['%s_%s_%s'%(station, parameter, units)])
                else:
                    for j in range(len(Data_df.loc[i]['date'])):
                        New_date.append(Data_df.loc[i]['date'][j])
                        New_data.append(Data_df.loc[i]['%s_%s_%s'%(station, parameter, units)][j])
            elif i not in Data_df.index:
                New_date.append(datetime.datetime(Monthly_df.loc[i]['date'].year, Monthly_df.loc[i]['date'].month, 1))
                New_data.append(np.NaN)

        New_date = pd.to_datetime(New_date, format = '%Y-%m-%d')
        Days = New_date.strftime("%d").astype(float)
        for i in range (len(Days)):
            if i == 0:
                Days_cum.append(Days[i])
            elif New_date[i].month == New_date[i-1].month:
                Days_cum.append(Days_cum[i-1]+(Days[i]-Days[i-1]))
            elif New_date[i].month != New_date[i-1].month:
                Days_cum.append(Days_cum[i-1]+Days[i]+monthrange(New_date[i-1].year, New_date[i-1].month)[1]-Days[i-1])
        Final_df = pd.DataFrame()
        Final_df['date'] = New_date
        Final_df['Data'] = New_data
        Final_df['Days'] = Days
        Final_df['Days_cum'] = Days_cum
        # Final_df.to_csv('C:/Work/Research/LOONE/Nitrogen Module/Interpolated_Data/In-Lake/L008_DO_No_Months_Missing_Trial.csv')  # noqa: E501
        #Remove Negative Data Values
        Final_df = Final_df[Final_df['Data'] >= 0]
        Final_df['date'] = pd.to_datetime(Final_df['date'], format = '%Y-%m-%d')
        start_date = Final_df['date'].iloc[0]
        end_date = Final_df['date'].iloc[-1]
        date_rng_TSS_1 = pd.date_range(start=start_date, end = end_date, freq= 'D')
        #Create a data frame with a date column
        Data_df = pd.DataFrame(date_rng_TSS_1, columns =['date'])
        Data_len = len(Data_df.index)
        Cum_days = np.zeros(Data_len)
        Data_daily = np.zeros(Data_len)
        #Set initial values
        Cum_days[0] = Data_df['date'].iloc[0].day
        Data_daily[0] = Final_df['Data'].iloc[0]
        for i in range (1, Data_len):
            Cum_days[i] = Cum_days[i-1]+1
            #Data_daily[i] = interpolate.interp1d(Final_df['Days'], Final_df['TSS'] , kind = 'linear')(Cum_days[i])
            Data_daily[i] = np.interp(Cum_days[i], Final_df['Days_cum'], Final_df['Data'])
        Data_df['Data'] = Data_daily
        Data_df.to_csv(f'{workspace}/{name}_Interpolated.csv', index=False)


def interpolate_all(workspace: str, d: dict = INTERP_DICT) -> None:
    """Interpolate all needed files for Lake Okeechobee

    Args:
        workspace (str): Path to files location.
        d (dict, optional): Dict with parameter key, units, and station IDs. Defaults to INTERP_DICT.
    """
    for param, values in d.items():
        print(f"Interpolating parameter: {param} for station IDs: {values['station_ids']}.")
        data_interpolations(workspace, param, values["units"], values["station_ids"])


def kinematic_viscosity(workspace: str, in_file_name: str, out_file_name: str = 'nu_20082023.csv'):
    # Read Mean H2O_T in LO
    LO_Temp = pd.read_csv(os.path.join(workspace, in_file_name))
    LO_T = LO_Temp['Water_T']

    n = len(LO_T.index)

    class nu_Func:

        def nu(T):
            nu20 = 1.0034/1E6 # m2/s (kinematic viscosity of water at T = 20 C)
            def func(x):
                # return[log(x[0]/nu20)-((20-T)/(T+96))*(1.2364-1.37E-3*(20-T)+5.7E-6*(20-T)**2)]
                return[(x[0]/nu20)-10**(((20-T)/(T+96))*(1.2364-1.37E-3*(20-T)+5.7E-6*(20-T)**2))]
            sol = fsolve(func, [9.70238995692062E-07])
            nu = sol[0]
            return(nu)

    nu = np.zeros(n, dtype = object)

    for i in range(n):
        nu[i] = nu_Func.nu(LO_T[i])

    nu_df = pd.DataFrame(LO_Temp['date'], columns=['date'])
    nu_df['nu'] = nu
    nu_df.to_csv(os.path.join(workspace, out_file_name), index=False)


def wind_induced_waves(
    workspace: str, wind_speed_in: str = "LOWS.csv",
    lo_stage_in: str = "LO_Stg_Sto_SA_2008-2023.csv" ,
    wind_shear_stress_out: str = "WindShearStress.csv",
    current_shear_stress_out: str ="Current_ShearStress.csv"
):
    # Read Mean Wind Speed in LO
    LO_WS = pd.read_csv(os.path.join(f'{workspace}/', wind_speed_in))
    LO_WS['WS_mps'] = LO_WS['LO_Avg_WS_MPH']*0.44704  # MPH to m/s
    #Read LO Stage to consider water depth changes
    LO_Stage = pd.read_csv(os.path.join(f'{workspace}/', lo_stage_in))
    LO_Stage['Stage_m'] = LO_Stage['Stage_ft'] * 0.3048
    Bottom_Elev = 0.5 # m (Karl E. Havens • Alan D. Steinman 2013)
    LO_Wd = LO_Stage['Stage_m'] - Bottom_Elev
    g = 9.81  # m/s2 gravitational acc
    d = 1.5  # m  LO Mean water depth
    F = 57500  # Fetch length of wind (m) !!!!!!
    nu = 1.0034/1E6 # m2/s (kinematic viscosity of water at T = 20 C)
    ru = 1000  # kg/m3

    n = len(LO_WS.index)

    class Wind_Func:
        def H(g, d, F, WS):
            H = (0.283*np.tanh(0.53*(g*d/WS**2)**0.75)*np.tanh(0.00565*(g*F/WS**2)**0.5/np.tanh(0.53*(g*d/WS**2)**(3/8))))*WS**2/g  # noqa: E501
            return(H)

        def T(g, d, F, WS):
            T = (7.54*np.tanh(0.833*(g*d/WS**2)**(3/8))*np.tanh(0.0379*(g*F/WS**2)**0.5/np.tanh(0.833*(g*d/WS**2)**(3/8))))*WS/g  # noqa: E501
            return(T)

        def L(g, d, T):

            def func(x):
                return[(g*T**2/2*np.pi)*np.tanh(2*np.pi*d/x[0]) - x[0]]
            sol = fsolve(func, [1])
            L = sol[0]
            return(L)

    W_H = np.zeros(n, dtype = object)
    W_T = np.zeros(n, dtype = object)
    W_L = np.zeros(n, dtype = object)
    W_ShearStress = np.zeros(n, dtype = object)
    for i in range(n):
        W_H[i] = Wind_Func.H(g, LO_Wd[i], F, LO_WS['WS_mps'].iloc[i])
        W_T[i] = Wind_Func.T(g, LO_Wd[i], F, LO_WS['WS_mps'].iloc[i])
        W_L[i] = Wind_Func.L(g, LO_Wd[i], W_T[i])
        W_ShearStress[i] = W_H[i]*(ru*(nu*(2*np.pi/W_T[i])**3)**0.5)/(2*np.sinh(2*np.pi*LO_Wd[i]/W_L[i]))


    Wind_ShearStress = pd.DataFrame(LO_WS['date'], columns=['date'])
    Wind_ShearStress['ShearStress'] = W_ShearStress*10  # Convert N/m2 to Dyne/cm2
    Wind_ShearStress.to_csv(os.path.join(workspace, wind_shear_stress_out), index=False)

    # #Monthly
    # Wind_ShearStress['Date'] = pd.to_datetime(Wind_ShearStress['Date'])
    # Wind_ShearStress_df = pd.DataFrame()
    # Wind_ShearStress_df['Date'] = Wind_ShearStress['Date'].dt.date
    # Wind_ShearStress_df['ShearStress'] = pd.to_numeric(Wind_ShearStress['ShearStress'])
    # Wind_ShearStress_df = Wind_ShearStress_df.set_index(['Date'])
    # Wind_ShearStress_df.index = pd.to_datetime(Wind_ShearStress_df.index, unit = 'ns')
    # Wind_ShearStress_df = Wind_ShearStress_df.resample('M').mean()
    # Wind_ShearStress_df.to_csv('C:/Work/Research/Data Analysis/Lake_O_Weather_Data/WindSpeed_Processed/WindShearStress_M.csv')  # noqa: E501

    # The drag coefficient
    CD = 0.001 * (0.75+0.067*LO_WS['WS_mps'])
    air_ru = 1.293  # kg/m3
    def tau_w(WS, CD, air_ru):
        tau_w = air_ru * CD * (WS**2)
        return tau_w

    def Current_bottom_shear_stress(ru, tau_w):
        # Constants
        kappa = 0.41  # Von Karman constant
        # Calculate the bottom friction velocity
        u_b = math.sqrt(tau_w / ru)
        # Calculate the bottom shear stress
        tau_b = ru * kappa**2 * u_b**2
        return tau_b
    Current_Stress = np.zeros(n, dtype = object)
    Wind_Stress = np.zeros(n, dtype = object)
    for i in range(n):
        Wind_Stress[i] = tau_w(LO_WS['WS_mps'].iloc[i], CD[i], air_ru)
        Current_Stress[i] = Current_bottom_shear_stress(ru, Wind_Stress[i])

    Current_ShearStress_df = pd.DataFrame(LO_WS['date'], columns=['date'])
    Current_ShearStress_df['Current_Stress'] = Current_Stress*10  # Convert N/m2 to Dyne/cm2
    Current_ShearStress_df['Wind_Stress'] = Wind_Stress*10  # Convert N/m2 to Dyne/cm2
    Current_ShearStress_df['Wind_Speed_m/s'] = LO_WS['WS_mps']

    def Current_bottom_shear_stress_2(u, k, nu, ks, z, ru):
        def func1(u_str1):
            return[u_str1[0]-u*k*np.exp(z/(0.11*nu/u_str1[0]))]
        sol1 = fsolve(func1, [1])
        def func2(u_str2):
            return[u_str2[0]-u*k*np.exp(z/(0.0333*ks))]
        sol2 = fsolve(func2, [1])
        def func3(u_str3):
            return[u_str3[0]-u*k*np.exp(z/((0.11*nu/u_str3[0])+0.0333*ks))]
        sol3 = fsolve(func3, [1])
        if sol1[0]*ks/nu <=5:
            u_str = sol1[0]
        elif sol2[0]*ks/nu >= 70:
            u_str = sol2[0]
        elif sol3[0]*ks/nu > 5 and sol3[0]*ks/nu < 70:
            u_str = sol3[0]
        tau_c = ru * u_str**2
        return(tau_c)

    def Current_bottom_shear_stress_3(u, k, nu, ks, z, ru):
        def func1(u_str1):
            return[u_str1[0]-u*k*(1/np.log(z/(0.11*nu/u_str1[0])))]
        sol1 = fsolve(func1, [1])
        def func2(u_str2):
            return[u_str2[0]-u*k*(1/np.log(z/(0.0333*ks)))]
        sol2 = fsolve(func2, [1])
        def func3(u_str3):
            return[u_str3[0]-u*k*(1/np.log(z/((0.11*nu/u_str3[0])+0.0333*ks)))]
        sol3 = fsolve(func3, [1])
        if sol1[0]*ks/nu <=5:
            u_str = sol1[0]
        elif sol2[0]*ks/nu >= 70:
            u_str = sol2[0]
        elif sol3[0]*ks/nu > 5 and sol3[0]*ks/nu < 70:
            u_str = sol3[0]
        else:
            u_str = 0
        tau_c = ru * u_str**2
        return(tau_c)
    ks = 5.27E-4  # m
    current_stress_3 = np.zeros(n, dtype = object)
    for i in range(n):
        current_stress_3[i] = Current_bottom_shear_stress_3(0.05, 0.41, nu, ks, LO_Wd[i], ru)
    Current_ShearStress_df['Current_Stress_3'] = current_stress_3*10  # Convert N/m2 to Dyne/cm2
    Current_ShearStress_df.to_csv(os.path.join(workspace, current_shear_stress_out), index=False)


def stg2sto(stg_sto_data_path: str, v: pd.Series, i: int) -> interpolate.interp1d:
        stgsto_data = pd.read_csv(stg_sto_data_path)
        #NOTE: We Can use cubic interpolation instead of linear
        x = stgsto_data['Stage']
        y = stgsto_data['Storage']
        if i == 0:
        #return storage given stage
            return interpolate.interp1d(x, y, fill_value='extrapolate', kind = 'linear')(v)
        else:
        #return stage given storage
            return interpolate.interp1d(y, x, fill_value='extrapolate', kind = 'linear')(v)


def stg2ar(stgar_data_path: str, v: pd.Series, i: int) -> interpolate.interp1d:
    import pandas as pd
    from scipy import interpolate
    stgar_data = pd.read_csv(stgar_data_path)
    #NOTE: We Can use cubic interpolation instead of linear
    x = stgar_data['Stage']
    y = stgar_data['Surf_Area']
    if i == 0:
        #return surface area given stage
        return interpolate.interp1d(x, y, fill_value='extrapolate', kind = 'linear')(v)
    else:
        #return stage given surface area
        return interpolate.interp1d(y, x, fill_value='extrapolate', kind = 'linear')(v)


@retry(Exception, tries=3, delay=15, backoff=2)
def get_pi(workspace: str) -> None:
    #Weekly data is downloaded from:
    #https://www.ncei.noaa.gov/access/monitoring/weekly-palmers/pdi-0804.csv
    #State:Florida Division:4.South Central
    df = pd.read_csv("https://www.ncei.noaa.gov/access/monitoring/weekly-palmers/pdi-0804.csv")
    df.to_csv(os.path.join(workspace, "PI.csv"))


if __name__ == "__main__":
    if sys.argv[1] == "get_dbkeys":
        get_dbkeys(sys.argv[2].strip("[]").replace(" ", "").split(','), *sys.argv[3:])
    elif sys.argv[1] == "data_interp":
        interp_args = [x for x in sys.argv[2:]]
        interp_args[0] = interp_args[0].rstrip("/")
        if len(interp_args) == 4:
            interp_args[3].strip("[]").replace(" ", "").split(',')
        data_interpolations(interp_args)
    elif sys.argv[1] == "interp_all":
        interpolate_all(sys.argv[2].rstrip("/"))
    elif sys.argv[1] == "kinematic_viscosity":
        kinematic_viscosity(sys.argv[2].rstrip("/"), *sys.argv[3:])
    elif sys.argv[1] == "wind_induced_waves":
        wind_induced_waves(sys.argv[2].rstrip("/"), *sys.argv[3:])
    elif sys.argv[1] == "get_pi":
        get_pi(sys.argv[2])
