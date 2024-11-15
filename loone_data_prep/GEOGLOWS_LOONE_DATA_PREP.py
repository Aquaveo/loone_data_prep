# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 00:18:50 2023

@author: osama
"""
import sys
import os
import shutil
from glob import glob
import pandas as pd
import numpy as np
from loone_data_prep.data_analyses_fns import DF_Date_Range
from loone_data_prep.utils import stg2sto, stg2ar
import datetime

START_DATE = datetime.datetime.now()
END_DATE = START_DATE + datetime.timedelta(days=15)

M3_Yr = 2008
M3_M = 1
M3_D = 1
D2_Yr = 2007
D2_M = 12
D2_D = 30
St_Yr = 2008
St_M = 1
St_D = 1
En_Yr = 2023
En_M = 3
En_D = 31

st_year = START_DATE.strftime("%Y")
st_month = START_DATE.strftime("%m")
st_day = START_DATE.strftime("%d")

end_year = END_DATE.strftime("%Y")
end_month = END_DATE.strftime("%m")
end_day = END_DATE.strftime("%d")


def main(input_dir: str, output_dir: str, ensemble_number: str) -> None:  # , historical_files_src: str) -> None:
    # To create File (Average_LO_Storage)
    # Read LO Average Stage (ft)
    LO_Stage = pd.read_csv(f"{input_dir}/LO_Stage.csv")
    # Create Column (EOD Stg(ft, NGVD)) in File (SFWMM_Daily_Outputs)
    LO_Stage = DF_Date_Range(LO_Stage, M3_Yr, M3_M, M3_D, En_Yr, En_M, En_D)
    LO_Stage.index = LO_Stage["date"]
    # Calculate average
    if "Average_Stage" not in LO_Stage.columns:
        LO_Stage = LO_Stage.loc[:, ~LO_Stage.columns.str.contains("^Unnamed")]
        LO_Stage["Average_Stage"] = LO_Stage.drop(columns=['date']).mean(axis=1)
        LO_Stage.to_csv(f"{input_dir}/LO_Stage.csv", index=False)
    LO_Storage = stg2sto(f"{input_dir}/StgSto_data.csv", LO_Stage["Average_Stage"], 0)
    LO_SA = stg2ar(f"{input_dir}/Stgar_data.csv", LO_Stage["Average_Stage"], 0)
    LO_Stg_Sto_SA_df = pd.DataFrame(LO_Stage["date"], columns=["date"])
    LO_Stg_Sto_SA_df["Stage_ft"] = LO_Stage["Average_Stage"]
    LO_Stg_Sto_SA_df["Stage_m"] = LO_Stg_Sto_SA_df["Stage_ft"].values * 0.3048  # ft to m
    LO_Stg_Sto_SA_df["Storage_acft"] = LO_Storage
    LO_Stg_Sto_SA_df["Storage_cmd"] = LO_Stg_Sto_SA_df["Storage_acft"] * 1233.48  # acft to m3/d
    LO_Stg_Sto_SA_df["SA_acres"] = LO_SA  # acres

    # Using geoglows data for S65_total, only data from S65E_S (none from S65EX1_S)
    S65_total = pd.read_csv(f"{input_dir}/S65E_S_FLOW_cmd_geoglows.csv")

    S71_S = pd.read_csv(f"{input_dir}/S71_S_FLOW_cmd_geoglows.csv")
    # S72_S = pd.read_csv(f'{input_dir}/S72_S_FLOW_cmd.csv')
    S84_S = pd.read_csv(f"{input_dir}/S84_S_FLOW_cmd_geoglows.csv")
    # S127_C = pd.read_csv(f'{input_dir}/S127_C_FLOW_cmd.csv')
    # S127_P = pd.read_csv(f'{input_dir}/S127_P_FLOW_cmd.csv')
    S129_C = pd.read_csv(f"{input_dir}/S129_C_FLOW_cmd_geoglows.csv")
    S129_P = pd.read_csv(f"{input_dir}/S129 PMP_P_FLOW_cmd_geoglows.csv")
    S133_P = pd.read_csv(f"{input_dir}/S133_P_FLOW_cmd_geoglows.csv")
    S135_C = pd.read_csv(f"{input_dir}/S135_C_FLOW_cmd_geoglows.csv")
    S135_P = pd.read_csv(f"{input_dir}/S135 PMP_P_FLOW_cmd_geoglows.csv")
    S154_C = pd.read_csv(f"{input_dir}/S154_C_FLOW_cmd_geoglows.csv")
    # S191_S = pd.read_csv(f'{input_dir}/S191_S_FLOW_cmd.csv')
    S308 = pd.read_csv(f"{input_dir}/S308.DS_FLOW_cmd_geoglows.csv")
    S351_S = pd.read_csv(f"{input_dir}/S351_S_FLOW_cmd_geoglows.csv")
    S352_S = pd.read_csv(f"{input_dir}/S352_S_FLOW_cmd_geoglows.csv")
    S354_S = pd.read_csv(f"{input_dir}/S354_S_FLOW_cmd_geoglows.csv")
    FISHP = pd.read_csv(f"{input_dir}/FISHP_FLOW_cmd_geoglows.csv")
    # L8 = pd.read_csv(f'{input_dir}/L8.441_FLOW_cmd_geoglows.csv')
    S2_P = pd.read_csv(f"{input_dir}/S2_P_FLOW_cmd_geoglows.csv")
    S3_P = pd.read_csv(f"{input_dir}/S3_P_FLOW_cmd_geoglows.csv")
    # S4_P = pd.read_csv(f'{input_dir}/S4_P_FLOW_cmd.csv')

    S77_S = pd.read_csv(f"{input_dir}/S77_S_FLOW_cmd_geoglows.csv")
    INDUST = pd.read_csv(f"{input_dir}/INDUST_FLOW_cmd_geoglows.csv")

    # Read Interpolated TP data
    # Data_Interpolation Python Script is used to interpolate TP data for all inflow stations addressed below!
    S65_total_TP = pd.read_csv(f"{input_dir}/S65E_S_PHOSPHATE_predicted.csv")[
        ["date", f"ensemble_{ensemble_number}_m^3/d"]
    ]
    S71_TP = pd.read_csv(f"{input_dir}/S71_S_PHOSPHATE_predicted.csv")[["date", f"ensemble_{ensemble_number}_m^3/d"]]
    # S72_TP = pd.read_csv(f'{input_dir}/S72_S_PHOSPHATE_predicted.csv')[['date', f'ensemble_{ensemble_number}_m^3/d']]
    S84_TP = pd.read_csv(f"{input_dir}/S84_S_PHOSPHATE_predicted.csv")[["date", f"ensemble_{ensemble_number}_m^3/d"]]
    # S127_TP = pd.read_csv(f'{input_dir}/S127_C_PHOSPHATE_predicted.csv')[['date', f'ensemble_{ensemble_number}_m^3/d']]
    S133_TP = pd.read_csv(f"{input_dir}/S133_P_PHOSPHATE_predicted.csv")[["date", f"ensemble_{ensemble_number}_m^3/d"]]
    S135_TP = pd.read_csv(f"{input_dir}/S135_C_PHOSPHATE_predicted.csv")[["date", f"ensemble_{ensemble_number}_m^3/d"]]
    S154_TP = pd.read_csv(f"{input_dir}/S154_C_PHOSPHATE_predicted.csv")[["date", f"ensemble_{ensemble_number}_m^3/d"]]
    # S191_TP = pd.read_csv(f'{input_dir}/S191_S_PHOSPHATE_predicted.csv')[['date', f'ensemble_{ensemble_number}_m^3/d']]
    # S308_TP = pd.read_csv(f'{input_dir}/water_quality_S308C_PHOSPHATE, TOTAL AS P_Interpolated.csv')[['date', 'Data']]
    FISHP_TP = pd.read_csv(f"{input_dir}/FISHP_PHOSPHATE_predicted.csv")[["date", f"ensemble_{ensemble_number}_m^3/d"]]
    # L8_TP = pd.read_csv(f'{input_dir}/water_quality_CULV10A_PHOSPHATE, TOTAL AS P_Interpolated.csv')[['date', f'ensemble_{ensemble_number}_m^3/d']] # ? Missing
    # S4_TP = pd.read_csv(f'{input_dir}/S4_P_PHOSPHATE_predicted.csv')[['date', f'ensemble_{ensemble_number}_m^3/d']]

    # Set date range for S65 TP
    S65_total_TP = DF_Date_Range(S65_total_TP, M3_Yr, M3_M, M3_D, En_Yr, En_M, En_D)

    # Set Date Range
    Q_names = [
        "S65_Q",
        "S71_Q",  #'S72_Q',
        "S84_Q",  #'S127_C_Q', 'S127_P_Q',
        "S129_C_Q",
        "S129_P_Q",
        "S133_P_Q",
        "S135_C_Q",
        "S135_P_Q",
        "S154_Q",  #'S191_Q',
        "S308_Q",
        "S351_Q",
        "S352_Q",
        "S354_Q",
        "FISHP_Q",  #'L8_Q',
        "S2_P_Q",
        "S3_P_Q",  #'S4_P_Q',
        "S77_Q",
        "INDUST_Q",
    ]
    Q_list = {
        "S65_Q": S65_total,
        "S71_Q": S71_S,
        "S84_Q": S84_S,
        "S129_C_Q": S129_C,
        "S129_P_Q": S129_P,
        "S133_P_Q": S133_P,
        "S135_C_Q": S135_C,
        "S135_P_Q": S135_P,
        "S154_Q": S154_C,
        "S308_Q": S308,
        "S351_Q": S351_S,
        "S352_Q": S352_S,
        "S354_Q": S354_S,
        "FISHP_Q": FISHP,  #'L8_Q': L8,
        "S2_P_Q": S2_P,
        "S3_P_Q": S3_P,
        "S77_Q": S77_S,
        "INDUST_Q": INDUST,
    }
    # Identify date range
    date = pd.date_range(start=f"{st_month}/{st_day}/{st_year}", end=f"{end_month}/{end_day}/{end_year}", freq="D")
    historical_date = pd.date_range(start=f"{M3_M}/{M3_D}/{M3_Yr}", end=f"{En_M}/{En_D}/{En_Yr}", freq="D")

    # Create Flow Dataframe
    # Flow_df = pd.read_csv(f'{output_dir}/Flow_df_3MLag.csv')
    # Flow_df = pd.DataFrame(historical_date, columns=["date"])
    # for i in range(len(Q_names)):
    #     x = DF_Date_Range(Q_list[Q_names[i]], M3_Yr, M3_M, M3_D, En_Yr, En_M, En_D)
    #     if len(x.iloc[:, -1:].values) == len(Flow_df["date"]):
    #         Flow_df[Q_names[i]] = x.iloc[:, -1:].values
    #     else:
    #         x.rename(columns={x.columns[-1]: Q_names[i]}, inplace=True)
    #         Flow_df = pd.merge(Flow_df, x[["date", Q_names[i]]], on="date", how="left")

    geoglows_flow_df = pd.DataFrame(date, columns=["date"])

    for i in range(len(Q_names)):
        x = DF_Date_Range(Q_list[Q_names[i]], st_year, st_month, st_day, end_year, end_month, end_day)
        for column_name in x.columns:
            if ensemble_number in column_name:
                geoglows_flow_df[Q_names[i]] = x[column_name]

    _create_flow_inflow_cqpq(geoglows_flow_df, ensemble_number, "S129_C_Q", "S129_P_Q", "S129_In")
    _create_flow_inflow_cqpq(geoglows_flow_df, ensemble_number, "S135_C_Q", "S135_P_Q", "S135_In")

    _create_flow_inflow_q(geoglows_flow_df, ensemble_number, "S308_Q", "S308_In")
    _create_flow_inflow_q(geoglows_flow_df, ensemble_number, "S77_Q", "S77_In")
    _create_flow_inflow_q(geoglows_flow_df, ensemble_number, "S351_Q", "S351_In")
    _create_flow_inflow_q(geoglows_flow_df, ensemble_number, "S352_Q", "S352_In")
    _create_flow_inflow_q(geoglows_flow_df, ensemble_number, "S354_Q", "S354_In")
    # _create_flow_inflow_q(geoglows_flow_df, ensemble_number, 'L8_Q', 'L8_In')

    _create_flow_outflow_q(geoglows_flow_df, ensemble_number, "S308_Q", "S308_Out")
    _create_flow_outflow_q(geoglows_flow_df, ensemble_number, "S77_Q", "S77_Out")
    _create_flow_outflow_q(geoglows_flow_df, ensemble_number, "INDUST_Q", "INDUST_Out")
    _create_flow_outflow_q(geoglows_flow_df, ensemble_number, "S351_Q", "S351_Out")
    _create_flow_outflow_q(geoglows_flow_df, ensemble_number, "S352_Q", "S352_Out")
    _create_flow_outflow_q(geoglows_flow_df, ensemble_number, "S354_Q", "S354_Out")
    # _create_flow_outflow_q(geoglows_flow_df, ensemble_number, 'L8_Q', 'L8_Out')

    geoglows_flow_df["Inflows"] = geoglows_flow_df[
        [
            "S65_Q",
            "S71_Q",  #'S72_Q',
            "S84_Q",  #'S127_In',
            "S129_In",
            "S133_P_Q",
            "S135_In",
            "S154_Q",  #'S191_Q',
            "S308_In",
            "S77_In",
            "S351_In",
            "S352_In",
            "S354_In",  #'L8_In',
            "FISHP_Q",
            "S2_P_Q",
            "S3_P_Q",
        ]
    ].sum(
        axis=1
    )  # , 'S4_P_Q']].sum(axis=1)
    geoglows_flow_df["Netflows"] = geoglows_flow_df["Inflows"] - geoglows_flow_df["INDUST_Out"]
    # flow_filter_cols = ["S308_Out", "S77_Out", 'S351_Out', 'S352_Out', 'S354_Out', 'INDUST_Out', 'L8_Out']
    flow_filter_cols = ["S308_Out", "S77_Out", "S351_Out", "S352_Out", "S354_Out", "INDUST_Out"]

    geoglows_flow_df["Outflows"] = geoglows_flow_df[flow_filter_cols].sum(axis=1)
    TP_names = [
        "S65_TP",
        "S71_TP",  #'S72_TP',
        "S84_TP",  #'S127_TP',
        "S133_TP",
        "S135_TP",
        "S154_TP",  #'S191_TP',
        # 'S308_TP',
        "FISHP_TP",
    ]  # , 'L8_TP']  #, 'S4_TP']
    TP_list = {
        "S65_TP": S65_total_TP,
        "S71_TP": S71_TP,  #'S72_TP': S72_TP,
        "S84_TP": S84_TP,  #'S127_TP': S127_TP,
        "S133_TP": S133_TP,
        "S135_TP": S135_TP,
        "S154_TP": S154_TP,  #'S191_TP': S191_TP,
        # 'S308_TP': S308_TP,
        "FISHP_TP": FISHP_TP,
    }  # , 'L8_TP': L8_TP}, 'S4_TP': S4_TP}
    # Create TP Concentrations Dataframe
    TP_Loads_In = pd.DataFrame(date, columns=["date"])
    for i in range(len(TP_names)):
        y = DF_Date_Range(TP_list[TP_names[i]], st_year, st_month, st_day, end_year, end_month, end_day)
        TP_Loads_In[TP_names[i]] = y[f"ensemble_{ensemble_number}_m^3/d"]

    # Calculate the total External Loads to Lake Okeechobee
    TP_Loads_In["External_P_Ld_mg"] = TP_Loads_In.sum(axis=1, numeric_only=True)

    # Create File (LO_External_Loadings_3MLag)
    TP_Loads_In_3MLag = DF_Date_Range(TP_Loads_In, st_year, st_month, st_day, end_year, end_month, end_day)
    TP_Loads_In_3MLag_df = pd.DataFrame(TP_Loads_In_3MLag["date"], columns=["date"])
    TP_Loads_In_3MLag_df["TP_Loads_In_mg"] = TP_Loads_In_3MLag["External_P_Ld_mg"]
    TP_Loads_In_3MLag_df["Atm_Loading_mg"] = [95890410.96] * len(TP_Loads_In_3MLag_df)

    # Create File (LO_Inflows_BK)
    LO_Inflows_BK = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    LO_Inflows_BK["Inflows_cmd"] = geoglows_flow_df["Inflows"]

    # Create File (Outflows_consd_20082023)
    Outflows_consd = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    Outflows_consd["Outflows_acft"] = geoglows_flow_df["Outflows"] / 1233.48  # acft

    # Create File (INDUST_Outflow_20082023)
    INDUST_Outflows = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    INDUST_Outflows["INDUST"] = geoglows_flow_df["INDUST_Out"]

    # Create File (Netflows_acft)
    # This is also Column (Net Inflow) in File (SFWMM_Daily_Outputs)
    Netflows = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    Netflows["Netflows_acft"] = geoglows_flow_df["Netflows"] / 1233.48  # acft

    # Create File (TotalQWCA_Obs)
    # This is also Column (RegWCA) in File (SFWMM_Daily_Outputs)
    TotalQWCA = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    TotalQWCA["S351_Out"] = geoglows_flow_df["S351_Out"] * (35.3147 / 86400)  # cmd to cfs
    TotalQWCA["S354_Out"] = geoglows_flow_df["S354_Out"] * (35.3147 / 86400)
    TotalQWCA["RegWCA_cfs"] = TotalQWCA.sum(axis=1, numeric_only=True)  # cfs
    TotalQWCA["RegWCA_acft"] = TotalQWCA["RegWCA_cfs"] * 1.9835  # acft

    # # Create Column (RegL8C51) in the File (SFWMM_Daily_Outputs)
    # L8C51 = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    # L8C51["S352_Out"] = geoglows_flow_df["S352_Out"].values * (35.3147 / 86400)  # cmd to cfs
    # L8C51["L8_O_cfs"] = geoglows_flow_df["L8_Out"].values * (35.3147 / 86400)  # cmd to cfs
    # L8C51["L8C51_cfs"] = L8C51.sum(axis=1)  # cfs
    # L8C51.to_csv(f"{output_dir}/L8C51.csv", index=False)

    # # C43 RO C44 RO
    # # Create Files (C43RO, C43RO_Monthly, C44RO, C44RO_Monthly)
    # # As well as Columns C43Runoff and C44Runoff in File (SFWMM_Daily_Outputs)
    # s79_path = glob(f'{input_dir}/S79_*FLOW*geoglows.csv')[0]
    # s80_path = glob(f'{input_dir}/S80_*FLOW*geoglows.csv')[0]
    # S79 = pd.read_csv(s79_path)
    # S80 = pd.read_csv(s80_path)
    # S79['Q_cmd'] = S79[f'ensemble_{ensemble_number}_m^3/d']  # already in cmd * 0.0283168466 * 86400
    # S80['Q_cmd'] = S80['ensemble_{ensemble_number}_m^3/d']  # already in cmd * 0.0283168466 * 86400

    # C43RO_df = pd.DataFrame(S79['date'], columns=['date'])
    # C44RO_df = pd.DataFrame(S79['date'], columns=['date'])
    # C43RO = np.zeros(len(C43RO_df.index))
    # C44RO = np.zeros(len(C44RO_df.index))
    # for i in range(len(C44RO_df.index)):
    #     if S79['Q_cmd'].iloc[i] - geoglows_flow_df['S77_Out'].iloc[i] + geoglows_flow_df['S77_In'].iloc[i] < 0:
    #         C43RO[i] = 0
    #     else:
    #         C43RO[i] = S79['Q_cmd'].iloc[i] - geoglows_flow_df['S77_Out'].iloc[i] + geoglows_flow_df['S77_In'].iloc[i]
    # for i in range(len(C44RO_df.index)):
    #     if S80['Q_cmd'].iloc[i] - geoglows_flow_df['S308_Out'].iloc[i] + geoglows_flow_df['S308_In'].iloc[i] < 0:
    #         C44RO[i] = 0
    #     else:
    #         C44RO[i] = S80['Q_cmd'].iloc[i] - geoglows_flow_df['S308_Out'].iloc[i] + geoglows_flow_df['S308_In'].iloc[i]
    # C43RO_df['C43RO_cmd'] = C43RO
    # C44RO_df['C44RO_cmd'] = C44RO
    # C43RO_df['C43RO_cfs'] = C43RO_df['C43RO_cmd']/(0.0283168466 * 86400)
    # C44RO_df['C44RO_cfs'] = C44RO_df['C44RO_cmd']/(0.0283168466 * 86400)
    # C43RO_df.to_csv(f'{output_dir}/C43RO.csv', index=False)
    # C44RO_df.to_csv(f'{output_dir}/C44RO.csv', index=False)

    # # Get monthly C43RO and C44RO from historical run
    # shutil.copyfile(os.path.join(historical_files_src, "C43RO_Monthly.csv"), os.path.join(output_dir, 'C43RO_Monthly.csv'))
    # shutil.copyfile(os.path.join(historical_files_src, "C44RO_Monthly.csv"), os.path.join(output_dir, 'C44RO_Monthly.csv'))

    # # SLTRIB
    # # Create File (SLTRIB_Monthly)
    # S48_S_path = glob(f'{input_dir}/S48_*FLOW*geoglows.csv')[0]
    # S49_S_path = glob(f'{input_dir}/S49_*FLOW*geoglows.csv')[0]
    # S48_S = pd.read_csv(S48_S_path)
    # S49_S = pd.read_csv(S49_S_path)
    # SLTRIB = pd.DataFrame(S48_S['date'], columns=['date'])
    # SLTRIB['SLTRIB_cmd'] = S48_S[f'ensemble_{ensemble_number}_m^3/d'] + S49_S[f'ensemble_{ensemble_number}_m^3/d']
    # SLTRIB['SLTRIB_cfs'] = SLTRIB['SLTRIB_cmd']/(0.0283168466 * 86400)

    # # Get monthly SLTRIB and Basin_RO from historical run
    # shutil.copyfile(os.path.join(historical_files_src, "SLTRIB_Monthly.csv"), os.path.join(output_dir, "SLTRIB_Monthly.csv"))
    # shutil.copyfile(os.path.join(historical_files_src, "Basin_RO_inputs.csv"), os.path.join(output_dir, "Basin_RO_inputs.csv"))

    # # EAA MIA RUNOFF
    # # Create File (EAA_MIA_RUNOFF_Inputs)
    # s3_path = glob(f"{input_dir}/S3_FLOW*geoglows.csv")[0]
    # s2_path = glob(f"{input_dir}/S2_NNR*FLOW*geoglows.csv")[0]
    # S3_Miami_data = pd.read_csv(s3_path)
    # S3_Miami = S3_Miami_data[f"ensemble_{ensemble_number}_m^3/d"]
    # S2_NNR_data = pd.read_csv(s2_path)
    # S2_NNR = S2_NNR_data[f"ensemble_{ensemble_number}_m^3/d"]
    # EAA_MIA_RO = pd.DataFrame(date, columns=["date"])
    # EAA_MIA_RO["MIA"] = S3_Miami.values / (0.0283168466 * 86400)
    # EAA_MIA_RO["NNR"] = S2_NNR.values / (0.0283168466 * 86400)
    # EAA_MIA_RO["WPB"] = geoglows_flow_df["S352_Out"] / (0.0283168466 * 86400)
    # EAA_MIA_RO["S2PMP"] = geoglows_flow_df["S2_P_Q"] / (0.0283168466 * 86400)
    # EAA_MIA_RO["S3PMP"] = geoglows_flow_df["S3_P_Q"] / (0.0283168466 * 86400)
    # EAA_MIA_RO.to_csv(f"{output_dir}/EAA_MIA_RUNOFF_Inputs.csv", index=False)

    # # Weekly Tributary Conditions
    # # Create File (Trib_cond_wkly_data)
    # # Net RF Inch
    # RF_data = pd.read_csv(f"{input_dir}/LAKE_RAINFALL_DATA.csv")
    # RF_data = DF_Date_Range(RF_data, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # ET_data = pd.read_csv(f"{input_dir}/LOONE_AVERAGE_ETPI_DATA.csv")
    # ET_data = DF_Date_Range(ET_data, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # Net_RF = pd.DataFrame(RF_data["date"], columns=["date"])
    # Net_RF = DF_Date_Range(Net_RF, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # Net_RF["NetRF_In"] = RF_data["average_rainfall"] - ET_data["average_ETPI"]
    # Net_RF = Net_RF.set_index(["date"])
    # Net_RF.index = pd.to_datetime(Net_RF.index, unit="ns")
    # Net_RF_Weekly = Net_RF.resample("W-FRI").sum()
    # # Net Inflows cfs
    # Net_Inflows = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    # Net_Inflows = DF_Date_Range(Net_Inflows, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # Net_Inflows["Net_Inflows"] = geoglows_flow_df["Netflows"] / (0.0283168466 * 86400)  # cmd to cfs
    # Net_Inflows = Net_Inflows.set_index(["date"])
    # Net_Inflows.index = pd.to_datetime(Net_Inflows.index, unit="ns")
    # Net_Inflow_Weekly = Net_Inflows.resample("W-FRI").mean()
    # # S65 cfs
    # S65E = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    # S65E = DF_Date_Range(S65E, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # S65E["S65E"] = geoglows_flow_df["S65_Q"] / (0.0283168466 * 86400)  # cmd to cfs
    # S65E = S65E.set_index(["date"])
    # S65E.index = pd.to_datetime(S65E.index, unit="ns")
    # S65E_Weekly = S65E.resample("W-FRI").mean()
    # # PI
    # # TODO
    # # This is prepared manually
    # # Weekly data is downloaded from https://www.ncei.noaa.gov/access/monitoring/weekly-palmers/time-series/0804
    # # State:Florida Division:4.South Central
    # PI = pd.DataFrame(S65E_Weekly.index, columns=["date"])
    # PI_data = pd.read_csv(f"{input_dir}/PI.csv")
    # PI["PI"] = PI_data.iloc[:, 1]

    # Trib_Cond_Wkly = pd.DataFrame(S65E_Weekly.index, columns=["date"])
    # Trib_Cond_Wkly["NetRF"] = Net_RF_Weekly["NetRF_In"].values
    # Trib_Cond_Wkly["NetInf"] = Net_Inflow_Weekly["Net_Inflows"].values
    # Trib_Cond_Wkly["S65E"] = S65E_Weekly["S65E"].values
    # Trib_Cond_Wkly["Palmer"] = PI["PI"].values
    # Trib_Cond_Wkly.to_csv(f"{output_dir}/Trib_cond_wkly_data.csv", index=False)

    # # Wind Speed
    # # Create File (LOWS)
    # L001WS = pd.read_csv(f"{input_dir}/L001_WNDS_MPH.csv")
    # L005WS = pd.read_csv(f"{input_dir}/L005_WNDS_MPH.csv")
    # L006WS = pd.read_csv(f"{input_dir}/L006_WNDS_MPH.csv")
    # LZ40WS = pd.read_csv(f"{input_dir}/LZ40_WNDS_MPH.csv")
    # L001WS = DF_Date_Range(L001WS, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # L005WS = DF_Date_Range(L005WS, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # L006WS = DF_Date_Range(L006WS, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # LZ40WS = DF_Date_Range(LZ40WS, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # LOWS = pd.DataFrame(L001WS["date"], columns=["date"])
    # LOWS["L001WS"] = L001WS["L001_WNDS_MPH"]
    # LOWS["L005WS"] = L005WS["L005_WNDS_MPH"]
    # LOWS["L006WS"] = L006WS["L006_WNDS_MPH"]
    # LOWS["LZ40WS"] = LZ40WS["LZ40_WNDS_MPH"]
    # LOWS["LO_Avg_WS_MPH"] = LOWS.mean(axis=1)
    # LOWS.to_csv(f"{output_dir}/LOWS.csv", index=False)

    # # RFVol acft
    # # Create File (RF_Volume)
    # RFVol = pd.DataFrame(RF_data["date"], columns=["date"])
    # RFVol["RFVol_acft"] = (RF_data["average_rainfall"].values / 12) * LO_Stg_Sto_SA_df["SA_acres"].values
    # RFVol.to_csv(f"{output_dir}/RFVol_LORS_20082023.csv", index=False)

    # # ETVol acft
    # # Create File (ETVol)
    # ETVol = pd.DataFrame(ET_data["date"], columns=["date"])
    # ETVol["ETVol_acft"] = (ET_data["average_ETPI"].values / 12) * LO_Stg_Sto_SA_df["SA_acres"].values
    # ETVol.to_csv(f"{output_dir}/ETVol_LORS_20082023.csv", index=False)

    # # WCA Stages
    # # Create File (WCA_Stages_Inputs)
    # Stg_3ANW = pd.read_csv(f"{input_dir}/Stg_3ANW.csv")
    # Stg_3ANW = DF_Date_Range(Stg_3ANW, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # Stg_2A17 = pd.read_csv(f"{input_dir}/Stg_2A17.csv")
    # Stg_2A17 = DF_Date_Range(Stg_2A17, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # Stg_3A3 = pd.read_csv(f"{input_dir}/Stg_3A3.csv")
    # Stg_3A3 = DF_Date_Range(Stg_3A3, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # Stg_3A4 = pd.read_csv(f"{input_dir}/Stg_3A4.csv")
    # Stg_3A4 = DF_Date_Range(Stg_3A4, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # Stg_3A28 = pd.read_csv(f"{input_dir}/Stg_3A28.csv")
    # Stg_3A28 = DF_Date_Range(Stg_3A28, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # WCA_Stg = pd.DataFrame(Stg_3A28["date"], columns=["date"])
    # WCA_Stg["3A-NW"] = Stg_3ANW["3A-NW_STG_ft NGVD29"].values
    # WCA_Stg["2A-17"] = Stg_2A17["2-17_GAGHT_feet"].values
    # WCA_Stg["3A-3"] = Stg_3A3["3-63_GAGHT_feet"].values
    # WCA_Stg["3A-4"] = Stg_3A4["3-64_GAGHT_feet"].values
    # WCA_Stg["3A-28"] = Stg_3A28["3-65_GAGHT_feet"].values
    # WCA_Stg.to_csv(f"{output_dir}/WCA_Stages_Inputs.csv", index=False)

    # # Predict Water Temp Function of Air Temp
    # L001_H2OT = pd.read_csv(f"{input_dir}/L001_H2OT_Degrees Celsius.csv")
    # L001_H2OT = DF_Date_Range(L001_H2OT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # L005_H2OT = pd.read_csv(f"{input_dir}/L005_H2OT_Degrees Celsius.csv")
    # L005_H2OT = DF_Date_Range(L005_H2OT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # L006_H2OT = pd.read_csv(f"{input_dir}/L006_H2OT_Degrees Celsius.csv")
    # L006_H2OT = DF_Date_Range(L006_H2OT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # LZ40_H2OT = pd.read_csv(f"{input_dir}/LZ40_H2OT_Degrees Celsius.csv")
    # LZ40_H2OT = DF_Date_Range(LZ40_H2OT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)

    # Water_Temp_data = pd.DataFrame(L001_H2OT["date"], columns=["date"])

    # Water_Temp_data["L001_WaterT"] = L001_H2OT["L001_H2OT_Degrees Celsius"]
    # Water_Temp_data["L005_WaterT"] = L005_H2OT["L005_H2OT_Degrees Celsius"]
    # Water_Temp_data["L006_WaterT"] = L006_H2OT["L006_H2OT_Degrees Celsius"]
    # Water_Temp_data["LZ40_WaterT"] = LZ40_H2OT["LZ40_H2OT_Degrees Celsius"]

    # Water_Temp_data = DF_Date_Range(Water_Temp_data, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # water_temp_filter_cols = ["L001_WaterT", "L005_WaterT", "L006_WaterT", "LZ40_WaterT"]
    # Water_Temp_data["WaterT_Mean"] = Water_Temp_data[water_temp_filter_cols].mean(axis=1)

    # L001_AirT = pd.read_csv(f"{input_dir}/L001_AIRT_Degrees Celsius.csv")
    # L001_AirT = DF_Date_Range(L001_AirT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # L005_AirT = pd.read_csv(f"{input_dir}/L005_AIRT_Degrees Celsius.csv")
    # L005_AirT = DF_Date_Range(L005_AirT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # L006_AirT = pd.read_csv(f"{input_dir}/L006_AIRT_Degrees Celsius.csv")
    # L006_AirT = DF_Date_Range(L006_AirT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # LZ40_AirT = pd.read_csv(f"{input_dir}/LZ40_AIRT_Degrees Celsius.csv")
    # LZ40_AirT = DF_Date_Range(LZ40_AirT, St_Yr, St_M, St_D, En_Yr, En_M, En_D)

    # WaterT_pred_df = pd.DataFrame(L001_AirT["date"], columns=["date"])

    # WaterT_pred_df["L001_WaterT_pred"] = 1.862667 + 0.936899 * L001_AirT["L001_AIRT_Degrees Celsius"].values
    # WaterT_pred_df["L005_WaterT_pred"] = 1.330211 + 0.909713 * L005_AirT["L005_AIRT_Degrees Celsius"].values
    # WaterT_pred_df["L006_WaterT_pred"] = -0.88564 + 1.01585 * L006_AirT["L006_AIRT_Degrees Celsius"].values
    # WaterT_pred_df["LZ40_WaterT_pred"] = 0.388231 + 0.980154 * LZ40_AirT["LZ40_AIRT_Degrees Celsius"].values
    # water_t_pred_filter_cols = ["L001_WaterT_pred", "L005_WaterT_pred", "L006_WaterT_pred", "LZ40_WaterT_pred"]
    # WaterT_pred_df["WaterT_pred_Mean"] = WaterT_pred_df[water_t_pred_filter_cols].mean(axis=1)
    # WaterT_pred_df_1 = DF_Date_Range(WaterT_pred_df, St_Yr, St_M, St_D, 2020, 8, 25)
    # WaterT_pred_df_2 = DF_Date_Range(WaterT_pred_df, 2020, 8, 26, En_Yr, En_M, En_D)
    # Filled_WaterT_1 = np.zeros(len(WaterT_pred_df_1.index))
    # Filled_WaterT_2 = np.zeros(len(WaterT_pred_df_2.index))
    # for i in range(len(Water_Temp_data.index)):
    #     if np.isnan(Water_Temp_data["WaterT_Mean"].iloc[i]):
    #         Filled_WaterT_1[i] = WaterT_pred_df_1["WaterT_pred_Mean"].iloc[i]
    #     else:
    #         Filled_WaterT_1[i] = Water_Temp_data["WaterT_Mean"].iloc[i]

    # Filled_WaterT_2 = WaterT_pred_df_2["WaterT_pred_Mean"]
    # Filled_WaterT_1df = pd.DataFrame(WaterT_pred_df_1["date"], columns=["date"])
    # Filled_WaterT_2df = pd.DataFrame(WaterT_pred_df_2["date"], columns=["date"])
    # Filled_WaterT_1df["Water_T"] = Filled_WaterT_1
    # Filled_WaterT_2df["Water_T"] = Filled_WaterT_2
    # Filled_WaterT = pd.concat([Filled_WaterT_1df, Filled_WaterT_2df]).reset_index(drop=True)
    # Filled_WaterT.to_csv(f"{output_dir}/Filled_WaterT_20082023.csv", index=False)

    # # TP Observations in Lake
    # L001_TP = pd.read_csv(f"{input_dir}/water_quality_L001_PHOSPHATE, TOTAL AS P.csv")
    # L004_TP = pd.read_csv(f"{input_dir}/water_quality_L004_PHOSPHATE, TOTAL AS P.csv")
    # L005_TP = pd.read_csv(f"{input_dir}/water_quality_L005_PHOSPHATE, TOTAL AS P.csv")
    # L006_TP = pd.read_csv(f"{input_dir}/water_quality_L006_PHOSPHATE, TOTAL AS P.csv")
    # L007_TP = pd.read_csv(f"{input_dir}/water_quality_L007_PHOSPHATE, TOTAL AS P.csv")
    # L008_TP = pd.read_csv(f"{input_dir}/water_quality_L008_PHOSPHATE, TOTAL AS P.csv")
    # LZ40_TP = pd.read_csv(f"{input_dir}/water_quality_LZ40_PHOSPHATE, TOTAL AS P.csv")

    # LO_TP_data = pd.merge(L001_TP, L004_TP, how="left", on="date")
    # LO_TP_data = pd.merge(LO_TP_data, L005_TP, how="left", on="date")
    # LO_TP_data = pd.merge(LO_TP_data, L006_TP, how="left", on="date")
    # LO_TP_data = pd.merge(LO_TP_data, L007_TP, how="left", on="date")
    # LO_TP_data = pd.merge(LO_TP_data, L008_TP, how="left", on="date")
    # LO_TP_data = pd.merge(LO_TP_data, LZ40_TP, how="left", on="date")
    # LO_TP_data = LO_TP_data.loc[:, ~LO_TP_data.columns.str.startswith("Unnamed")]
    # LO_TP_data["Mean_TP"] = LO_TP_data.mean(axis=1)
    # LO_TP_data = LO_TP_data.set_index(["date"])
    # LO_TP_data.index = pd.to_datetime(LO_TP_data.index, unit="ns")
    # LO_TP_Monthly = LO_TP_data.resample("M").mean()
    # LO_TP_Monthly.to_csv(f"{output_dir}/LO_TP_Monthly.csv")

    # # Interpolated TP Observations in Lake
    # L001_TP_Inter = pd.read_csv(f"{input_dir}/water_quality_L001_PHOSPHATE, TOTAL AS P_Interpolated.csv")
    # L004_TP_Inter = pd.read_csv(f"{input_dir}/water_quality_L004_PHOSPHATE, TOTAL AS P_Interpolated.csv")
    # L005_TP_Inter = pd.read_csv(f"{input_dir}/water_quality_L005_PHOSPHATE, TOTAL AS P_Interpolated.csv")
    # L006_TP_Inter = pd.read_csv(f"{input_dir}/water_quality_L006_PHOSPHATE, TOTAL AS P_Interpolated.csv")
    # L007_TP_Inter = pd.read_csv(f"{input_dir}/water_quality_L007_PHOSPHATE, TOTAL AS P_Interpolated.csv")
    # L008_TP_Inter = pd.read_csv(f"{input_dir}/water_quality_L008_PHOSPHATE, TOTAL AS P_Interpolated.csv")
    # LZ40_TP_Inter = pd.read_csv(f"{input_dir}/water_quality_LZ40_PHOSPHATE, TOTAL AS P_Interpolated.csv")

    # LO_TP_data_Inter = pd.merge(L001_TP_Inter, L004_TP_Inter, how="left", on="date")
    # LO_TP_data_Inter = pd.merge(LO_TP_data_Inter, L005_TP_Inter, how="left", on="date")
    # LO_TP_data_Inter = pd.merge(LO_TP_data_Inter, L006_TP_Inter, how="left", on="date")
    # LO_TP_data_Inter = pd.merge(LO_TP_data_Inter, L007_TP_Inter, how="left", on="date")
    # LO_TP_data_Inter = pd.merge(LO_TP_data_Inter, L008_TP_Inter, how="left", on="date")
    # LO_TP_data_Inter = pd.merge(LO_TP_data_Inter, LZ40_TP_Inter, how="left", on="date")
    # LO_TP_data_Inter = LO_TP_data_Inter.loc[:, ~LO_TP_data_Inter.columns.str.startswith("Unnamed")]
    # LO_TP_data_Inter["Mean_TP"] = LO_TP_data_Inter.mean(axis=1)
    # LO_TP_data_Inter = LO_TP_data_Inter.set_index(["date"])
    # LO_TP_data_Inter.index = pd.to_datetime(LO_TP_data_Inter.index, unit="ns")
    # LO_TP_Monthly_Inter = LO_TP_data_Inter.resample("M").mean()
    # Max = LO_TP_Monthly_Inter.max(axis=1)
    # Min = LO_TP_Monthly_Inter.min(axis=1)
    # LO_TP_Monthly_Inter["Max"] = Max.values
    # LO_TP_Monthly_Inter["Min"] = Min.values
    # LO_TP_Monthly_Inter.to_csv(f"{output_dir}/LO_TP_Monthly.csv")

    # # Interpolated OP Observations in Lake
    # # Create File (LO_Avg_OP)
    # L001_OP_Inter = pd.read_csv(f"{input_dir}/water_quality_L001_PHOSPHATE, ORTHO AS P_Interpolated.csv")
    # L004_OP_Inter = pd.read_csv(f"{input_dir}/water_quality_L004_PHOSPHATE, ORTHO AS P_Interpolated.csv")
    # L005_OP_Inter = pd.read_csv(f"{input_dir}/water_quality_L005_PHOSPHATE, ORTHO AS P_Interpolated.csv")
    # L006_OP_Inter = pd.read_csv(f"{input_dir}/water_quality_L006_PHOSPHATE, ORTHO AS P_Interpolated.csv")
    # L007_OP_Inter = pd.read_csv(f"{input_dir}/water_quality_L007_PHOSPHATE, ORTHO AS P_Interpolated.csv")
    # L008_OP_Inter = pd.read_csv(f"{input_dir}/water_quality_L008_PHOSPHATE, ORTHO AS P_Interpolated.csv")
    # LZ40_OP_Inter = pd.read_csv(f"{input_dir}/water_quality_LZ40_PHOSPHATE, ORTHO AS P_Interpolated.csv")

    # LO_OP_data_Inter = pd.merge(L001_OP_Inter, L004_OP_Inter, how="left", on="date")
    # LO_OP_data_Inter = pd.merge(LO_OP_data_Inter, L005_OP_Inter, how="left", on="date")
    # LO_OP_data_Inter = pd.merge(LO_OP_data_Inter, L006_OP_Inter, how="left", on="date")
    # LO_OP_data_Inter = pd.merge(LO_OP_data_Inter, L007_OP_Inter, how="left", on="date")
    # LO_OP_data_Inter = pd.merge(LO_OP_data_Inter, L008_OP_Inter, how="left", on="date")
    # LO_OP_data_Inter = pd.merge(LO_OP_data_Inter, LZ40_OP_Inter, how="left", on="date")
    # LO_OP_data_Inter = LO_OP_data_Inter.loc[:, ~LO_OP_data_Inter.columns.str.startswith("Unnamed")]
    # LO_OP_data_Inter["Mean_OP"] = LO_OP_data_Inter.mean(axis=1)
    # LO_OP_data_Inter = DF_Date_Range(LO_OP_data_Inter, St_Yr, St_M, St_D, En_Yr, En_M, En_D)
    # LO_OP_data_Inter.to_csv(f"{output_dir}/LO_OP.csv", index=False)

    # Write Data into csv files
    # write Avg Stage (ft, m) Storage (acft, m3) SA (acres) to csv
    LO_Stg_Sto_SA_df.to_csv(f"{output_dir}/Average_LO_Storage_3MLag.csv", index=False)
    # Write S65 TP concentrations (mg/L)
    S65_total_TP.to_csv(f"{output_dir}/S65_TP_3MLag.csv", index=False)
    # TP External Loads 3 Months Lag (mg)
    TP_Loads_In_3MLag_df.to_csv(f"{output_dir}/LO_External_Loadings_3MLag_{ensemble_number}.csv", index=False)
    # Flow dataframe including Inflows, NetFlows, and Outflows (all in m3/day)
    geoglows_flow_df.to_csv(f"{output_dir}/geoglows_flow_df_ens_{ensemble_number}_predicted.csv", index=False)
    # Inflows (cmd)
    LO_Inflows_BK.to_csv(f"{output_dir}/LO_Inflows_BK.csv", index=False)
    # Outflows (cmd)
    Outflows_consd.to_csv(f"{output_dir}/Outflows_consd.csv", index=False)
    # NetFlows (cmd)
    #Netflows.to_csv(f"{output_dir}/Netflows_acft.csv", index=False)
    # # Total flows to WCAs (acft)
    TotalQWCA.to_csv(f"{output_dir}/TotalQWCA_Obs.csv", index=False)
    # INDUST Outflows (cmd)
    INDUST_Outflows.to_csv(f"{output_dir}/INDUST_Outflows.csv", index=False)


def _create_flow_inflow_cqpq(
    df: pd.DataFrame, ensemble_number: str, column_cq: str, column_pq: str, column_sum_name: str
):
    """Creates the inflow columns for the given column_cq column. For flows with (*_C_Q, *_P_Q). Handles ensembles.

    Args:
        df (pd.DataFrame): The pandas DataFrame to add the new columns to. Also holds the input columns.
        column_cq (str): The name of the C_Q column to create the inflow columns from. Don't include the ensemble part of the name.
        column_pq (str): The name of the P_Q column to create the inflow columns from. Don't include the ensemble part of the name.
        column_sum_name (str): The name of the created inflow columns. Don't include the ensemble part of the name.
    """
    # Create the inflow column for each ensemble
    column_cq_e = column_cq
    column_pq_e = column_pq
    column_sum_name_e = column_sum_name

    df[column_cq_e] = df[column_cq_e][df[column_cq_e] >= 0]
    df[column_cq_e] = df[column_cq_e].fillna(0)
    df[column_sum_name_e] = df[[column_cq_e, column_pq_e]].sum(axis=1)


def _create_flow_inflow_q(df: pd.DataFrame, ensemble_number: str, column_q: str, column_in: str):
    """Creates the inflow columns for the given column_q column. For flows with (*_Q). Handles ensembles.

    Args:
        df (pd.DataFrame): The pandas DataFrame to add the new column to.
        column_q (str): The name of the *_Q column to create the inflow columns from. Don't include the ensemble part of the name.
        column_in (str): The name of the created inflow column. Don't include the ensemble part of the name.
    """
    column_q_e = column_q
    column_in_e = column_in

    df[column_in_e] = df[column_q_e][df[column_q_e] < 0]
    df[column_in_e] = df[column_in_e] * -1
    df[column_in_e] = df[column_in_e].fillna(0)


def _create_flow_outflow_q(df: pd.DataFrame, ensemble_number: str, column_q: str, column_out: str):
    """Creates the outflow columns for the given column_q column. For flows with (*_Q). Handles ensembles.

    Args:
        df (pd.DataFrame): The pandas DataFrame to add the new column to.
        column_q (str): The name of the *_Q column to create the outflow columns from. Don't include the ensemble part of the name.
        column_out (str): The name of the created outflow column. Don't include the ensemble part of the name.
    """

    column_q_e = column_q
    column_out_e = column_out

    df[column_out_e] = df[column_q_e][df[column_q_e] >= 0]
    df[column_out_e] = df[column_out_e].fillna(0)


if __name__ == "__main__":
    main(sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/"), sys.argv[3])  # , sys.argv[4])
