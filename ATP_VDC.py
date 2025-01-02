__author__ = "Najmul"
import sys
import warnings
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import sys

# exl_path = sys.argv[1]
# out_path = sys.argv[2]
# file_name = sys.argv[3]
#
# if not exl_path.endswith('/') and not exl_path.endswith('\\'):
#     exl_path += '/'
#
# if not out_path.endswith('/') and not out_path.endswith('\\'):
#     out_path += '/'

# Ignore all warnings
warnings.filterwarnings("ignore")
now = datetime.now()
print(now)
today = now.today()
today = today.strftime("%Y-%m-%d")
today = datetime.strptime(today, "%Y-%m-%d")
# out_path = out_path + "/" + file_name + ".xlsx"
# df_input = pd.read_excel(exl_path + "INPUT.xlsx", index_col=False)
out_path = r"C:\Vodacom_TNZ\Pre&Post\\"
df_input = pd.read_excel(r"C:\Vodacom_TNZ\Pre&Post\\" + "INPUT.xlsx", index_col=False)
site_ids = df_input["Site ID"].unique().tolist()
activity_date = df_input["On Air"].unique().tolist()
# print("Site_Ids:", site_ids, "\nActivity_Date:", activity_date)



# sys.exit()
# site_id = input(("Enter Site ID:"))

###----------------------fetching data for 2G-----------------------
engine = create_engine('postgresql://postgres:12345@10.133.132.90:5432/cno_prod')
conn = engine.connect()
###-----Physical Database---------------------
sql_query = text(f"SELECT * FROM \"{'VDC_TNZ_2G_CELL_NED'}\"")
df_2G_phdb = pd.read_sql(sql_query, conn)
df_2G_phdb = df_2G_phdb[df_2G_phdb["SITE_ID"].isin(site_ids)]
# df_2G_phdb["key"] = df_2G_phdb["Element1"].astype(str) + df_2G_phdb["Element2"].astype(str)
df_2G_phdb.rename(columns={"CI": "key"}, inplace=True)
# print(df_2G_phdb.to_string())
cell_2G = df_2G_phdb["key"].unique().tolist()
print("2G Cell Ids:", cell_2G, len(cell_2G))
#####------raw reports------------------------------
# Convert list to a format suitable for SQL IN clause
cell_2G = ', '.join([f"'{element}'" for element in cell_2G])
# SQL query to fetch data for values in the list
sql_query = text(f'SELECT * FROM "VDC_TNZ_2G_CELL_DAY" WHERE "Primary_Key" IN ({cell_2G})')
# Fetch data into a DataFrame
df_2G = pd.read_sql(sql_query, conn)
filter_2G_list = ["Date", "Primary_Key", "Element1", "Element2", "Element3", "CSSR, VOICE", "SDCCH BLOCKING RATE",
               "TCH CALL BLOCKING", "SDCCH DROP RATIO WITHOUT T3101", "HANDOVER SUCCESS RATE", "TBF DROP RATE",
               "DL TBF EST SUCC RATE", "UL TBF EST SUCC RATE", "NEW_NED_DCR",
               "DL CUMULATIVE QUALITY RATIO IN CLASS X", "UL CUMULATIVE QUALITY RATIO IN CLASS 5", "TCH AVAILABILITY RATIO", "ACCESSIBILITY ATTEMPTS (NSN):", "HOSRDNOM", "NEW_NED_DCR_NOM"]
df_2G_all = df_2G[filter_2G_list]
df_2G_all = df_2G_all.round(2)
# print(df_2G_phdb.columns)
#####--------mapping band from phdb--------------------------------------
sql_query = text(f"SELECT * FROM \"{'VDC_TNZ_2G_PHDB'}\"")
df_2G_band = pd.read_sql(sql_query, conn)
df_2G_band["key"] = df_2G_band["Element1"].astype(str) + df_2G_band["Element2"].astype(str)
df_2G_all = pd.merge(df_2G_all, df_2G_band[["key", "BAND"]], how="left", left_on="Primary_Key", right_on="key")
# print(df_2G_all.head(10).to_string())
# sys.exit()
# Drop the duplicate key column if necessary
df_2G_all = df_2G_all.drop(columns=["key"])
#####----mapping counter kpi from VDC_TNZ_RPRT_2G_Nokia_10174DY---------------------
sql_query = text(f'SELECT * FROM "VDC_TNZ_RPRT_2G_Nokia_10174DY" WHERE "Primary_Key" IN ({cell_2G})')
# Fetch data into a DataFrame
df_2G_cntr = pd.read_sql(sql_query, conn)
df_2G_all = pd.merge(df_2G_all, df_2G_cntr[["Primary_Key", "SDBLOCKNOM", "TCHBLOCKNOM", "SDR4NOM", "TBFDROPNOM"]], how="left", on="Primary_Key")
# df_2G_all.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\df_2G_all.csv", index=False)
# df_2G_cntr.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\all_cntr.csv", index=False)
####------------------fetching data for 3G----------------------------------------

###-----Physical Database---------------------
sql_query = text(f"SELECT * FROM \"{'VDC_TNZ_3G_PHDB'}\"")
df_3G_band = pd.read_sql(sql_query, conn)
df_3G_band["Primary_Key"] = df_3G_band["Element1"].astype(str) + df_3G_band["Element2"].astype(str)

# Convert list to a format suitable for SQL IN clause
# site_id = ', '.join([f"'{element}'" for element in site_sec_id])
sql_query = text(f'SELECT * FROM "VDC_TNZ_3G_CELL_DAY" WHERE "Element2" = ANY(:site_ids)')
# Fetch data into a DataFrame
df_3G = pd.read_sql(sql_query, conn, params={"site_ids": site_ids})

df_3G["CSSR"] = (df_3G["CSSR_VOICE_NOM"] / df_3G["CSSR_VOICE_DENOM"]) * 100
df_3G["Call Setup Success Rate (PS Data)"] = (df_3G["CSSR_PS_NOM_1"] / df_3G["CSSR_PS_DENOM_EXCL_HIGHPRIO"]) * 100
df_3G["DROPPED PS DATA RATE (PS DATA)"] = (df_3G["PS_DROP_NOM"] / df_3G["PS_DROP_DENOM"]) * 100
df_3G["INTER SYSTEM HAND OVER RT"] = (df_3G["ISHO_SR_NOM"] / df_3G["ISHO_SR_DENOM"]) * 100
df_3G["nom"] = df_3G["SUCC_INTRA_INTRA_HHO_ATT_RT (M1008C55)"] + df_3G["SUCC_INTRA_INTER_HHO_ATT_RT (M1008C59)"]
df_3G["Denom"] = df_3G["SUCC_INTRA_INTER_HHO_ATT_RT (M1008C59)"] + df_3G["INTRA_INTER_HHO_ATT_RT (M1008C58)"]
df_3G["IFHO RT SR (%)"] = (df_3G["nom"] / df_3G["Denom"]) * 100
filter_3G_list = ["Date", "Primary_Key", "Element1", "Element2", "Element3", "DN", "RRC CONNECTION SETUP SUCCESS RATIO", "CSSR", "Call Setup Success Rate (PS Data)",
               "DROPPED PS DATA RATE (PS DATA)", "INTER SYSTEM HAND OVER RT", "IFHO RT SR (%)", "NED DCR 3G", "NED DROP CALL", "RRC_CONN_STP_SR_DENOM", "CSSR_VOICE_DENOM", "NEW_CSSR_PS_DENOM"]
df_3G_all = df_3G[filter_3G_list]
df_3G_all = df_3G_all.round(2)
# df_3G_all = pd.merge(df_3G_all, df_3G_phdb[["Primary_Key", "BAND"]], how="left", on="Primary_Key")
# # df_3G_all["Primary_Key"] = df_2G_all["Primary_Key"].astype(str)
df_3G_all.drop(columns=["Primary_Key"], inplace=True)
df_3G_all["Primary_Key"] = df_3G_all["Element2"].astype(str) + df_3G_all["Element3"].astype(str)
cell_id = df_3G_all["Primary_Key"].unique().tolist()
print("3G Cell Ids:", cell_id, len(cell_id))
df_3G_all = pd.merge(df_3G_all, df_3G_band[["Primary_Key", "BAND"]], how="left", on="Primary_Key")
# print(df_2G_all.head(10).to_string())
# Drop the duplicate key column if necessary
####-----------------fetching data for 4G---------------------

site_ids = ', '.join([f"'{element}'" for element in site_ids])
sql_query = text(f'SELECT * FROM "VDC_TNZ_4G_CELL_NED" WHERE "SITE_ID" IN ({site_ids})')
# Fetch data into a DataFrame
df_4G_phdb = pd.read_sql(sql_query, conn)
df_4G_phdb.rename(columns={"CELL_NAME": "LNCEL NAME"}, inplace=True)
# df_4G_phdb.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\phdb.csv", index=False)
lncel_list = df_4G_phdb["LNCEL NAME"].unique().tolist()
print("4G Cell CELL_NAME:", lncel_list, len(lncel_list))
# Convert list to a format suitable for SQL IN clause
# lncel_lists = ', '.join([f"'{element}'" for element in lncel_list])
sql_query = text(f'SELECT * FROM "VDC_TNZ_4G_CELL_DAY" WHERE "Element3" = ANY(:lncel_list)')
# Fetch data into a DataFrame
df_4G = pd.read_sql(sql_query, conn, params={"lncel_list": lncel_list})
# df_4G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\day.csv", index=False)

###------mapping site id and sector id---------------------------


# df_4G_phdb.drop(columns=["Element3"], inplace=True)
# df_4G_phdb.rename(columns={"LNCEL NAME": "Element3"}, inplace=True)
# df_4G = pd.merge(df_4G, df_4G_phdb[["Element3",  ]], how="left", on="Element3")
# df_4G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\day.csv", index=False)
filter_4G_list = ["Date", "Primary_Key", "Element1", "Element2", "Element3",  "TOTAL E-UTRAN RRC CONN STP SR", "E-UTRAN E-RAB STP SR", "E-RAB DR, RAN VIEW", "INTER ENB E-UTRAN HO SR X2", "E-UTRAN HO SR, INTER ENB S1", "INTRA ENB HO SR",
                  "TOTEUTRARRCCONSTPSR_DENOM_FL18", "ERABSTPSR_DENOM_FL18", "ERAB_DR_RAN_NOM_FL18", "INTERENB_HOSRX2_DENOM_FL18", "INTERENB_HOSRS1_DENOM_FL18",
                  "INTRAENBHOSR_DENOM_FL18", "PDCP SDU VOLUME, DL", "PDCP SDU VOLUME, UL", "AVG PDCP CELL THP UL", "INTRAENBHOSR_NOM_FL18",
                  "INTERENB_HOSRX2_NOM_FL18", "INTERENB_HOSRS1_NOM_FL18", "PERC DL PRB UTIL", "PERC UL PRB UTIL", "AVERAGE CQI", "DL_USER_THRPTFL18"]
# print(df_4G.columns)
df_4G_all = df_4G[filter_4G_list]
# List of columns to be converted to float
columns_to_convert = ["TOTAL E-UTRAN RRC CONN STP SR", "E-UTRAN E-RAB STP SR", "E-RAB DR, RAN VIEW", "INTER ENB E-UTRAN HO SR X2", "E-UTRAN HO SR, INTER ENB S1", "INTRA ENB HO SR",
                  "TOTEUTRARRCCONSTPSR_DENOM_FL18", "ERABSTPSR_DENOM_FL18", "ERAB_DR_RAN_NOM_FL18", "INTERENB_HOSRX2_DENOM_FL18", "INTERENB_HOSRS1_DENOM_FL18",
                  "INTRAENBHOSR_DENOM_FL18", "PDCP SDU VOLUME, DL", "PDCP SDU VOLUME, UL", "AVG PDCP CELL THP UL", "INTRAENBHOSR_NOM_FL18",
                  "INTERENB_HOSRX2_NOM_FL18", "INTERENB_HOSRS1_NOM_FL18", "PERC DL PRB UTIL", "PERC UL PRB UTIL", "AVERAGE CQI", "DL_USER_THRPTFL18"]  # Replace with your actual column names

# Convert specified columns to float
df_4G_all[columns_to_convert] = df_4G_all[columns_to_convert].astype(float)
###------------mapping band --------------------------------
sql_query = text(f"SELECT * FROM \"{'VDC_TNZ_4G_PHDB'}\"")
df_4G_band = pd.read_sql(sql_query, conn)
# df_4G_band["Primary_Key"] = df_4G_band["Element1"].astype(str) + df_4G_band["Element2"].astype(str)
df_4G_all = pd.merge(df_4G_all, df_4G_band[["LNCEL NAME", "BAND", "SECTOR ID"]], how="left", left_on="Element3", right_on="LNCEL NAME")
df_4G_all.drop(columns=["LNCEL NAME"], inplace=True)
###------------------------closing connection-----------------
conn.close()
####---------mapping site id into input-----------------------
# df_input.rename(columns={"Site ID": "SITE_ID"}, inplace=True)
df_input_2G = pd.merge(df_input, df_2G_phdb[["SITE_ID", "key"]], left_on="Site ID", right_on="SITE_ID", how="left")
# df_input_2G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\df_input_2G.csv", index=False)
df_input_4G = pd.merge(df_input, df_4G_phdb[["SITE_ID", "LNCEL NAME"]], left_on="Site ID", right_on="SITE_ID", how="left")
# df_input_4G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\df_input_4G.csv", index=False)
# sys.exit()
df_filtered_2G_list = []
summary_df_2G_list = []
avg2G_list = []
diff_avg2G_list = []
df_next_2G_7_color_list = []
df_filtered_3G_list = []
avg3G_list = []
diff_avg3G_list = []
df_next_3G_7_color_list = []
df_filtered_4G_list = []
avg4G_list = []
diff_avg4G_list = []
df_next_4G_7_color_list = []

###------------------------closing connection-----------------
conn.close()
for on_air in activity_date:
    print("Calculating 2G..........")
    on_air = [on_air]
    # print(on_air)
    df_filter = df_input_2G[df_input_2G['On Air'].isin(on_air)]
    # print(df_filter)
    activity_date = df_filter["On Air"].unique().tolist()
    date_ = activity_date[0]
    on_air_site = df_filter["Site ID"].unique().tolist()
    df_2G_key = df_input_2G[df_input_2G["Site ID"].astype(int).isin(on_air_site)]
    # print(df_2G_key.head(10).to_string())
    cellid_2g = df_2G_key["key"].unique().tolist()
    print("on_air_site", cellid_2g)
    df_2G = df_2G_all[df_2G_all["Primary_Key"].astype(int).isin(cellid_2g)]
    # df_2G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\df_2G.csv", index=False)
    # sys.exit()
    # df_2G = df_2G_key[df_2G_key["key"].isin(on_air_site)]
    # print("Activity_Date:", date_)
    specific_date = datetime.strptime(str(date_), "%Y-%m-%d %H:%M:%S")
    specific_date = specific_date.strftime("%Y-%m-%d")
    specific_date = datetime.strptime(specific_date, "%Y-%m-%d")
    # print(specific_date)
    # Convert the date column to datetime type
    df_2G['Date'] = pd.to_datetime(df_2G['Date'])
    # print(df_2G['Date'].head(5))
    # Find the previous Monday
    previous_monday = specific_date - timedelta(days=specific_date.weekday() + 7)
    # Find the next Monday
    # next_monday = today - timedelta(days=today.weekday() + 7)
    # Calculate the previous week's Monday to Friday
    previous_week_start = previous_monday
    previous_week_end = previous_monday + timedelta(days=4)  # Friday of the previous week
    # Calculate the next week's Monday to Friday
    # next_week_start = next_monday
    next_week_end = today - timedelta(days=7)  # last 7 days
    last_3_days = today - timedelta(days=3)
    print("previous_week_start", previous_week_start, "\nprevious_week_end",
          previous_week_end, "\nnext_week_start", next_week_end)
    df_filtered_2G = df_2G[((df_2G['Date'] >= previous_week_start) & (df_2G['Date'] <= previous_week_end)) |
                     (df_2G['Date'] >= next_week_end)]
    # print(df_filtered_2G['Date'].unique())
    ####------------summary--------------------------------
    summary_df_2G_date = df_filtered_2G.drop_duplicates(subset='Date')['Date']
    summary_df_2G_site = df_filtered_2G.drop_duplicates(subset='Primary_Key')['Primary_Key']
    # sys.exit()
    # Exclude Saturdays (5) and Sundays (6)
    # df_filtered_2G = df_filtered_2G[~df_filtered_2G['Date'].dt.weekday.isin([5, 6])]
    ####--------reindexing column--------------------------------------------------
    # Specify the column you want to move and its new position (2nd position in this example)
    column_to_move = 'BAND'
    # new_position = 1  # 0-based index, so 1 corresponds to the 2nd position
    new_position = 5
    # Extract the column to move
    column_series = df_filtered_2G.pop(column_to_move)
    # Insert the column at the new position
    df_filtered_2G.insert(new_position, column_to_move, column_series)
    ###-----------------------------------------------------------------
    df_next_7_2G = df_2G[(df_2G['Date'] >= next_week_end)]
    df_last_3_2G = df_2G[(df_2G['Date'] >= last_3_days)]
    # if site_sec_id == ["22771", "22772", "22773"]:
    #     print(site_sec_id)
    #     # print(df_last_3_2G.to_string())
    #     df_last_3_2G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg.csv", index=False)
    #     sys.exit()
    # print(df_last_3_2G.to_string())
    # sys.exit()
    ###--------making Average--------------------------------
    thresholds_2G = {
        'CSSR, VOICE': (99, ">="),  # threshold and operator
        'SDCCH BLOCKING RATE': (0.5, "<="),
        'TCH CALL BLOCKING': (0.5, "<="),
        'SDCCH DROP RATIO WITHOUT T3101': (1.5, "<="),
        'HANDOVER SUCCESS RATE': (98.2, ">="),
        'TBF DROP RATE': (1.5, "<="),
        'DL TBF EST SUCC RATE': (99.85, ">="),
        'UL TBF EST SUCC RATE': (99.45, ">="),
        'NEW_NED_DCR': (0.4, "<="),
        'DL CUMULATIVE QUALITY RATIO IN CLASS X': (98, ">="),
        'UL CUMULATIVE QUALITY RATIO IN CLASS 5': (98, ">="),
        'TCH AVAILABILITY RATIO': (98, ">=")

    }
    df_last_7_2G = df_2G[(df_2G['Date'] >= previous_week_start) & (df_2G['Date'] <= previous_week_end)]
    if df_last_7_2G.empty:
        pre_value = "Threshold_based"
        print(pre_value)
        avg2G_last_7 = df_next_7_2G[["Primary_Key", "Element1", "Element2", "Element3", "BAND"]]
        avg2G_last_7["Date"] = "avg2G_last_7"
        for key, value in thresholds_2G.items():
            avg2G_last_7[key] = value[0]

        avg2G_last_7.drop_duplicates(subset="Primary_Key", inplace=True)
        # avg2G_last_7.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg2G_last_7.csv", index=False)
    else:
        avg2G_last_7 = df_last_7_2G.groupby(
            ["Primary_Key", "Element1", "Element2", "Element3", "BAND"]).mean().reset_index()
        avg2G_last_7["Date"] = "avg2G_last_7"

    avg2G_next_7 = df_next_7_2G.groupby(["Primary_Key", "Element1", "Element2", "Element3", "BAND"]).mean().reset_index()
    avg2G_next_7["Date"] = "avg2G_next_7"
    avg2G = pd.concat([avg2G_last_7, avg2G_next_7], ignore_index=True)
    df_last_3_avg_2G = df_last_3_2G.groupby(["Primary_Key", "Element1", "Element2", "Element3", "BAND"]).mean().reset_index()
    ###----------making difference between last7 and next7(Post - Pre)-------------------------------------------------
    # Merge the DataFrames on the grouping columns
    diff_avg2G = pd.merge(
        avg2G_last_7,
        avg2G_next_7,
        on=["Primary_Key", "Element1", "Element2", "Element3", "BAND"],
        suffixes=('_last_7', '_next_7')
    )
    # if df_last_7_2G.empty:
    #     print("yes")
    #     avg2G_next_7.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg2G_next_7.csv", index=False)
    #     sys.exit()
    # df_last_3_avg_2G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg.csv", index=False)
    # diff_avg2G["Primary_Key"] = diff_avg2G["Primary_Key"].as
    diff_avg2G = pd.merge(diff_avg2G, df_last_3_avg_2G, on="Primary_Key", how="left")
    # print(diff_avg2G.columns)
    # if site_sec_id == ["22771", "22772", "22773"]:
    #     # print(diff_avg2G.to_string())
    #     diff_avg2G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\merge.csv", index=False)
    #     # sys.exit()
    columns_to_remove = ["Date", "Primary_Key", "Element1", "Element2", "Element3", "Date_last_7", "Date_next_7", "BAND"]
    # List of columns to format
    columns_to_diff = [col for col in diff_avg2G.columns if col not in columns_to_remove]
    # print(columns_to_diff)
    diff_col_list = []
    nom_dnm_mapping = {
        "CSSR, VOICE": "ACCESSIBILITY ATTEMPTS (NSN):",
        "SDCCH BLOCKING RATE": "SDBLOCKNOM",
        "TCH CALL BLOCKING": "TCHBLOCKNOM",
        "SDCCH DROP RATIO WITHOUT T3101": "SDR4NOM",
        "TBF DROP RATE": "TBFDROPNOM",
        "NEW_NED_DCR": "NEW_NED_DCR_NOM",
        "HANDOVER SUCCESS RATE": 'HOSRDNOM',
        # "DL TBF EST SUCC RATE": -0.5,
        # "UL TBF EST SUCC RATE": -0.5,
        # "DL CUMULATIVE QUALITY RATIO IN CLASS X": -0.5,
        # "UL CUMULATIVE QUALITY RATIO IN CLASS 5": -0.5,
        # "TCH AVAILABILITY RATIO": -0.5
    }
    for col in columns_to_diff:
        # print(col)
        if '_next_7' in col:  # Ensure only the "next_7" numeric columns are considered
            col_last_7 = col.replace('_next_7', '_last_7')
            col_last_3 = col.replace('_next_7', '')
            # print(col_last_7)
            # print(diff_avg2G.columns)
            # if df_last_7_2G.empty:
            #     print("Yes")
            #     diff_avg2G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\diff_avg2G.csv", index=False)
            #     sys.exit()
            diff_avg2G[f'{col}_diff'] = diff_avg2G[col].astype(float) - diff_avg2G[col_last_7].astype(float)
            status = f'{col}_diff'
            status = status.split("_next_7_diff")[0]
            # print(status)
            if status in ["CSSR, VOICE", "HANDOVER SUCCESS RATE", "DL TBF EST SUCC RATE", "UL TBF EST SUCC RATE",
                          "DL CUMULATIVE QUALITY RATIO IN CLASS X", "UL CUMULATIVE QUALITY RATIO IN CLASS 5",
                          "TCH AVAILABILITY RATIO"]:
                # Apply condition with -0.5 threshold
                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    # print(nom_dnm_pre, nom_dnm_post)
                    if df_last_7_2G.empty:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}' if
                            row[
                                f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}' if
                            row[
                                f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                else:
                    # print(diff_avg2G[[col_last_3]], site_sec_id)
                    if df_last_7_2G.empty:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}' if
                            row[
                                f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}' if
                            row[
                                f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )

            else:
                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    # print(nom_dnm_pre, nom_dnm_post)
                    if df_last_7_2G.empty:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}' if
                            row[
                                f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}' if
                            row[
                                f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                else:
                    if df_last_7_2G.empty:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}' if
                            row[
                                f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg2G[status] = diff_avg2G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}' if
                            row[
                                f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )

            diff_col_list.append(status)

    # print(diff_avg2G.columns)
    diff_avg2G = diff_avg2G[["Primary_Key", "Element1_y", "Element2_y", "Element3_y", "BAND_y"] + diff_col_list]
    diff_avg2G = diff_avg2G[["Primary_Key", "Element1_y", "Element2_y", "Element3_y", "BAND_y"] + diff_col_list]
    # Drop nom&dnom columns from the diff_avg2G
    # print(diff_avg2G.columns)
    try:
        exclude_columns = list(nom_dnm_mapping.values())
        diff_avg2G = diff_avg2G.drop(columns=exclude_columns)
    except:
        pass


    ########----------3G--------------------------------------------------------------
    print("Calculating 3G..........")
    df_3G = df_3G_all[df_3G_all["Element2"].astype(int).isin(on_air_site)]
    cellid_3g = df_3G["Primary_Key"].unique().tolist()
    print("on_air_cells", cellid_3g)
    # Convert the date column to datetime type
    df_3G['Date'] = pd.to_datetime(df_3G['Date'])
    # Filter data between previous week's Monday to Friday and next week's Monday to Friday
    df_filtered_3G = df_3G[((df_3G['Date'] >= previous_week_start) & (df_3G['Date'] <= previous_week_end)) |
                           (df_3G['Date'] >= next_week_end)]
    # Exclude Saturdays (5) and Sundays (6)
    # df_filtered_3G = df_filtered_3G[~df_filtered_3G['Date'].dt.weekday.isin([5, 6])]
    ###---------------------------------reindexing columns-------------------------
    # Specify the column you want to move and its new position (2nd position in this example)
    column_to_move = 'Primary_Key'
    # new_position = 1  # 0-based index, so 1 corresponds to the 2nd position
    new_position = 1
    # Extract the column to move
    column_series = df_filtered_3G.pop(column_to_move)
    # Insert the column at the new position
    df_filtered_3G.insert(new_position, column_to_move, column_series)
    # Specify the column you want to move and its new position (2nd position in this example)
    column_to_move = 'BAND'
    # new_position = 1  # 0-based index, so 1 corresponds to the 2nd position
    new_position = 5
    # Extract the column to move
    column_series = df_filtered_3G.pop(column_to_move)
    # Insert the column at the new position
    df_filtered_3G.insert(new_position, column_to_move, column_series)
    ##-----------------------------------------------------------------
    df_next_7_3G = df_3G[(df_3G['Date'] >= next_week_end)]
    # df_3G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\raw.csv")
    df_last_3_3G = df_3G[(df_3G['Date'] >= last_3_days)]
    thresholds_3G = {
        'RRC CONNECTION SETUP SUCCESS RATIO': (99.7, ">="),  # threshold and operator
        'CSSR': (99.55, ">="),
        'Call Setup Success Rate (PS Data)': (99.3, ">="),
        'DROPPED PS DATA RATE (PS DATA)': (0.4, "<="),
        'INTER SYSTEM HAND OVER RT': (97.5, ">="),
        'IFHO RT SR (%)': (97.5, ">="),
        'NED DCR 3G': (0.2, "<=")
    }
    # df_last_3_3G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\raw_avg.csv")
    ###--------making Average--------------------------------

    df_last_7_3G = df_3G[(df_3G['Date'] >= previous_week_start) & (df_3G['Date'] <= previous_week_end)]
    if df_last_7_3G.empty:
        pre_value = "Threshold_based"
        print(pre_value)
        avg3G_last_7 = df_next_7_3G[["Primary_Key", "Element1", "Element2", "Element3", "DN", "BAND"]]
        avg3G_last_7["Date"] = "avg3G_last_7"
        for key, value in thresholds_3G.items():
            avg3G_last_7[key] = value[0]

        avg3G_last_7.drop_duplicates(subset="Primary_Key", inplace=True)
        # avg2G_last_7.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg2G_last_7.csv", index=False)
    else:
        avg3G_last_7 = df_last_7_3G.groupby(
            ["Primary_Key", "Element1", "Element2", "Element3", "DN", "BAND"]).mean().reset_index()
        avg3G_last_7["Date"] = "avg3G_last_7"

    avg3G_next_7 = df_next_7_3G.groupby(["Primary_Key", "Element1", "Element2", "Element3", "DN", "BAND"]).mean().reset_index()
    avg3G_next_7["Date"] = "avg3G_next_7"
    avg3G = pd.concat([avg3G_last_7, avg3G_next_7], ignore_index=True)
    df_last_3_avg_3G = df_last_3_3G.groupby(
        ["Primary_Key", "Element1", "Element2", "Element3", "DN", "BAND"]).mean().reset_index()

    # print()
    # df_last_3_avg_3G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg.csv")
    ###----------making difference between last7 and next7(Post - Pre)-------------------------------------------------
    # Merge the DataFrames on the grouping columns
    diff_avg3G = pd.merge(
        avg3G_last_7,
        avg3G_next_7,
        on=["Primary_Key", "Element1", "Element2", "Element3", "DN", "BAND"],
        suffixes=('_last_7', '_next_7')
    )

    diff_avg3G = pd.merge(diff_avg3G, df_last_3_avg_3G, on="Primary_Key", how="left")
    # print(diff_avg3G.columns)
    # diff_avg3G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\diff_avg.csv", index=False)
    columns_to_remove = ["Date", "Primary_Key", "Element1", "Element2", "Element3", "Date_last_7", "Date_next_7", "BAND"]
    # List of columns to format
    columns_to_diff = [col for col in diff_avg3G.columns if col not in columns_to_remove]
    # print(columns_to_diff)
    diff_col_list = []
    nom_dnm_mapping = {
        "RRC CONNECTION SETUP SUCCESS RATIO": "RRC_CONN_STP_SR_DENOM",
        "Call Setup Success Rate (PS Data)": "NEW_CSSR_PS_DENOM",
        "CSSR": "CSSR_VOICE_DENOM",
        "NED DCR 3G": "NED DROP CALL"
    }
    for col in columns_to_diff:
        # print(col)
        if '_next_7' in col:  # Ensure only the "next_7" numeric columns are considered
            col_last_7 = col.replace('_next_7', '_last_7')
            col_last_3 = col.replace('_next_7', '')
            # if diff_avg3G[col_last_7].empty:
            #     # print("post3G", diff_avg3G[col])
            # print(col_last_7)
            diff_avg3G[f'{col}_diff'] = diff_avg3G[col].astype(float) - diff_avg3G[col_last_7].astype(float)
            status = f'{col}_diff'
            status = status.split("_next_7_diff")[0]
            # print(status)
            if status in ["RRC CONNECTION SETUP SUCCESS RATIO", "CSSR", "Call Setup Success Rate (PS Data)",
                          "INTER SYSTEM HAND OVER RT"]:
                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    if df_last_7_3G.empty:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                else:
                    if df_last_7_3G.empty:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
            elif status == "IFHO RT SR (%)":
                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    if df_last_7_3G.empty:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}'
                            if row[f'{col}_diff'] < -200 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}'
                            if row[f'{col}_diff'] < -200 else 'OK',
                            axis=1
                        )
                else:
                    if df_last_7_3G.empty:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] < -200 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] < -200 else 'OK',
                            axis=1
                        )
            else:
                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    if df_last_7_3G.empty:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                else:
                    if df_last_7_3G.empty:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg3G[status] = diff_avg3G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )

            diff_col_list.append(status)
    # print(diff_avg3G.columns)
    diff_avg3G = diff_avg3G[["Primary_Key", "Element1_y", "Element2_y", "Element3_y", "DN_y", "BAND_y"] + diff_col_list]
    # Drop nom&dnom columns from the diff_avg3G
    try:
        exclude_columns = list(nom_dnm_mapping.values())
        diff_avg3G = diff_avg3G.drop(columns=exclude_columns)
    except:
        pass
    ###--------------4G----------------------------------------------------
    print("Calculating 4G..........")
    df_4G_key = df_input_4G[df_input_4G["Site ID"].astype(int).isin(on_air_site)]
    # print(df_2G_key.head(10).to_string())
    cellid_4g = df_4G_key["LNCEL NAME"].unique().tolist()
    print("on_air_cells", cellid_4g)
    df_4G = df_4G_all[df_4G_all["Element3"].isin(cellid_4g)]
    # print(df_4G.columns)
    # sys.exit()
    # df_2G = df_2G_key[df_2G_key["key"].isin(on_air_site)]
    # print("Activity_Date:", date_)
    # specific_date = datetime.strptime(str(date_), "%Y-%m-%d %H:%M:%S")
    # specific_date = specific_date.strftime("%Y-%m-%d")
    # specific_date = datetime.strptime(specific_date, "%Y-%m-%d")
    # Convert the date column to datetime type
    df_4G['Date'] = pd.to_datetime(df_4G['Date'])
    # Filter data between previous week's Monday to Friday and next week's Monday to Friday
    df_filtered_4G = df_4G[((df_4G['Date'] >= previous_week_start) & (df_4G['Date'] <= previous_week_end)) |
                           (df_4G['Date'] >= next_week_end)]
    # Exclude Saturdays (5) and Sundays (6)
    # df_filtered_4G = df_filtered_4G[~df_filtered_4G['Date'].dt.weekday.isin([5, 6])]
    df_next_7_4G = df_4G[(df_4G['Date'] >= next_week_end)]
    df_last_3_4G = df_4G[(df_4G['Date'] >= last_3_days)]
    # print(df_next_7.to_string())
    # sys.exit()
    # df_filtered_2G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\last&next7days.csv", index=False)
    thresholds_4G = {
        'TOTAL E-UTRAN RRC CONN STP SR': (99.5, ">="),  # threshold and operator
        'E-UTRAN E-RAB STP SR': (99.5, ">="),
        'E-RAB DR, RAN VIEW': (0.3, "<="),
        'INTER ENB E-UTRAN HO SR X2': (98, ">="),
        'E-UTRAN HO SR, INTER ENB S1': (98, ">="),
        'INTRA ENB HO SR': (98, ">=")
    }
    # Function to color cells based on value, threshold, and operator
    ###--------making Average--------------------------------
    df_last_7_4G = df_4G[(df_4G['Date'] >= previous_week_start) & (df_4G['Date'] <= previous_week_end)]
    df_last_7_4G = df_last_7_4G.round(2)
    if df_last_7_4G.empty:
        pre_value = "Threshold_based"
        print(pre_value)
        avg4G_last_7 = df_next_7_4G[["Primary_Key", "Element1", "Element2", "Element3", "BAND", "SECTOR ID"]]
        avg4G_last_7["Date"] = "avg4G_last_7"
        for key, value in thresholds_4G.items():
            avg4G_last_7[key] = value[0]

        avg4G_last_7.drop_duplicates(subset="Primary_Key", inplace=True)
        # avg2G_last_7.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg2G_last_7.csv", index=False)
    else:
        avg4G_last_7 = df_last_7_4G.groupby(
            ["Primary_Key", "Element1", "Element2", "Element3", "BAND", "SECTOR ID"]).mean().reset_index()
        avg4G_last_7["Date"] = "avg4G_last_7"
    avg4G_next_7 = df_next_7_4G.groupby(
        ["Primary_Key", "Element1", "Element2", "Element3", "BAND", "SECTOR ID"]).mean().reset_index()
    avg4G_next_7["Date"] = "avg4G_next_7"
    avg4G = pd.concat([avg4G_last_7, avg4G_next_7], ignore_index=True)
    df_last_3_avg_4G = df_last_3_4G.groupby(
        ["Primary_Key", "Element1", "Element2", "Element3", "BAND", "SECTOR ID"]).mean().reset_index()

    ###----------making difference between last7 and next7(Post - Pre)-------------------------------------------------
    # Merge the DataFrames on the grouping columns
    diff_avg4G = pd.merge(
        avg4G_last_7,
        avg4G_next_7,
        on=["Primary_Key", "Element1", "Element2", "Element3", "BAND", "SECTOR ID"],
        suffixes=('_last_7', '_next_7')
    )
    diff_avg4G = pd.merge(diff_avg4G, df_last_3_avg_4G, on="Primary_Key",
                          how="left")

    columns_to_remove = ["Date", "Primary_Key", "Element1", "Element2", "Element3", "Date_last_7", "Date_next_7", "BAND", "SECTOR ID"]
    # List of columns to format
    columns_to_diff = [col for col in diff_avg4G.columns if col not in columns_to_remove]
    # print(columns_to_diff)
    diff_col_list = []
    nom_dnm_mapping = {
        "TOTAL E-UTRAN RRC CONN STP SR": "TOTEUTRARRCCONSTPSR_DENOM_FL18",
        "E-UTRAN E-RAB STP SR": "ERABSTPSR_DENOM_FL18",
        "E-RAB DR, RAN VIEW": "ERAB_DR_RAN_NOM_FL18",
        "INTER ENB E-UTRAN HO SR X2": "INTERENB_HOSRX2_DENOM_FL18",
        "E-UTRAN HO SR, INTER ENB S1": "INTERENB_HOSRS1_DENOM_FL18",
        "INTRA ENB HO SR": "INTRAENBHOSR_DENOM_FL18"
    }
    for col in columns_to_diff:
        # print(col)
        if '_next_7' in col:  # Ensure only the "next_7" numeric columns are considered
            col_last_7 = col.replace('_next_7', '_last_7')
            col_last_3 = col.replace('_next_7', '')
            # if diff_avg4G[col_last_7].empty:
            #     print("post4G", diff_avg4G[col])
            # print(col_last_7)
            diff_avg4G[f'{col}_diff'] = diff_avg4G[col].astype(float) - diff_avg4G[col_last_7].astype(float)
            status = f'{col}_diff'
            status = status.split("_next_7_diff")[0]
            # print(status)
            if status in ["TOTAL E-UTRAN RRC CONN STP SR", "E-UTRAN E-RAB STP SR", "INTER ENB E-UTRAN HO SR X2",
                          "E-UTRAN HO SR, INTER ENB S1", "INTRA ENB HO SR"]:

                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    if df_last_7_4G.empty:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                else:
                    if df_last_7_4G.empty:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] < -0.5 else 'OK',
                            axis=1
                        )

            elif status in ["PERC UL PRB UTIL", "PERC DL PRB UTIL", "PDCP SDU VOLUME, DL", "PDCP SDU VOLUME, UL"]:
                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    if df_last_7_4G.empty:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}'
                            if row[f'{col}_diff'] > 10 and row[f'{col}_diff'] < 10 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}'
                            if row[f'{col}_diff'] > 10 and row[f'{col}_diff'] < 10 else 'OK',
                            axis=1
                        )
                else:
                    if df_last_7_4G.empty:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] > 10 and row[f'{col}_diff'] < 10 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] > 10 and row[f'{col}_diff'] < 10 else 'OK',
                            axis=1
                        )
            else:
                nom_dnm = nom_dnm_mapping.get(status)
                if nom_dnm is not None:
                    nom_dnm_pre = nom_dnm + '_last_7'
                    nom_dnm_post = nom_dnm + '_next_7'
                    if df_last_7_4G.empty:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)} , {nom_dnm_pre}:{round(row[nom_dnm_pre], 2)}, {nom_dnm_post}:{round(row[nom_dnm_post], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                else:
                    if df_last_7_4G.empty:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, Threshold: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )
                    else:
                        diff_avg4G[status] = diff_avg4G.apply(
                            lambda
                                row: f'Not OK, pre: {round(row[col_last_7], 2)}, post: {round(row[col], 2)}, last_3_avg_{col_last_3}:{round(row[col_last_3], 2)}'
                            if row[f'{col}_diff'] > 0.5 else 'OK',
                            axis=1
                        )


            diff_col_list.append(status)

    diff_avg4G = diff_avg4G[["Primary_Key", "Element1_y", "Element2_y", "Element3_y", "BAND_y", "SECTOR ID_y"] + diff_col_list]
    # Drop nom&dnom columns from the diff_avg4G
    try:
        exclude_columns = list(nom_dnm_mapping.values())
        diff_avg4G = diff_avg4G.drop(columns=exclude_columns)
    except:
        pass



    ####----appending dataframes--------------------------------------------
    ####------2G----------------
    df_filtered_2G_list.append(df_filtered_2G)
    avg2G_list.append(avg2G)
    diff_avg2G_list.append(diff_avg2G)
    df_next_2G_7_color_list.append(df_next_7_2G)
    # summary_df_2G_list.append(summary_df_2G_date)
    # summary_df_2G_list.append(summary_df_2G_site)
    # summary_df_2G_list.append()
    ####----3G-----------------------------
    df_filtered_3G_list.append(df_filtered_3G)
    avg3G_list.append(avg3G)
    diff_avg3G_list.append(diff_avg3G)
    df_next_3G_7_color_list.append(df_next_7_3G)
    ####-----4G--------------------------------
    df_filtered_4G_list.append(df_filtered_4G)
    avg4G_list.append(avg4G)
    diff_avg4G_list.append(diff_avg4G)
    df_next_4G_7_color_list.append(df_next_7_4G)





#######--------------2G--------------------------------
df_filtered_2G = pd.concat(df_filtered_2G_list, axis=0)
avg2G = pd.concat(avg2G_list, axis=0)
avg2G["Date"] = avg2G["Date"].replace("avg2G_last_7", "Pre").replace("avg2G_next_7", "Post")
diff_avg2G = pd.concat(diff_avg2G_list, axis=0)
df_next_7_2G = pd.concat(df_next_2G_7_color_list, axis=0)
# summary_df = pd.concat(summary_df_2G_list, axis=0)
#######---------------3G----------------------------------
df_filtered_3G = pd.concat(df_filtered_3G_list, axis=0)
avg3G = pd.concat(avg3G_list, axis=0)
avg3G["Date"] = avg3G["Date"].replace("avg3G_last_7", "Pre").replace("avg3G_next_7", "Post")
diff_avg3G = pd.concat(diff_avg3G_list, axis=0)
df_next_7_3G = pd.concat(df_next_3G_7_color_list, axis=0)
######-----------4G----------------------------------
df_filtered_4G = pd.concat(df_filtered_4G_list, axis=0)
avg4G = pd.concat(avg4G_list, axis=0)
avg4G["Date"] = avg4G["Date"].replace("avg4G_last_7", "Pre").replace("avg4G_next_7", "Post")
diff_avg4G = pd.concat(diff_avg4G_list, axis=0)
df_next_7_4G = pd.concat(df_next_4G_7_color_list, axis=0)
###---making trends for prb, payload, thrpt, cqi-------------------------
# avg4G.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\avg4G.csv")
filter_avg4G = avg4G.dropna(subset=["SECTOR ID"])
bands = filter_avg4G["BAND"].unique()
filter_avg4G["SECTOR ID"] =filter_avg4G["SECTOR ID"].astype(int)
filter_avg4G["Site_ID_&_Sector"] = filter_avg4G["Element2"].astype(str) + "_" + filter_avg4G["SECTOR ID"].astype(str)
# pivot_table = filter_avg4G.pivot_table(index='Site_ID_&_Sector', columns='BAND', values='Element3')
filter_avg4G_pre = filter_avg4G[filter_avg4G["Date"].isin(["Pre"])]
filter_avg4G_post = filter_avg4G[filter_avg4G["Date"].isin(["Post"])]
pivot_table_pre = filter_avg4G_pre.pivot_table(index="Site_ID_&_Sector", columns="BAND", values="Element3",
                             aggfunc=lambda x: ','.join(x)).fillna('')
pivot_table_post = filter_avg4G_post.pivot_table(index="Site_ID_&_Sector", columns="BAND", values="Element3",
                             aggfunc=lambda x: ','.join(x)).fillna('')
pivot_table_pre = pivot_table_pre.reset_index()
pivot_table_post = pivot_table_post.reset_index()
for i in bands:
    # Merge on the column specified by i
    pivot_update_pre = pivot_table_pre.merge(filter_avg4G_pre[["Element3", "AVERAGE CQI", "DL_USER_THRPTFL18", "PDCP SDU VOLUME, DL", "PDCP SDU VOLUME, UL", "PERC DL PRB UTIL"]], left_on=i, right_on="Element3", how="left")
    # # Rename the merged column
    pivot_update_pre = pivot_update_pre.rename(columns={"AVERAGE CQI": f"Pre:AVERAGE CQI:{i}","DL_USER_THRPTFL18": f"Pre:DL_USER_THRPTFL18:{i}",
                            "PDCP SDU VOLUME, DL": f"Pre:PDCP SDU VOLUME, DL:{i}", "PDCP SDU VOLUME, UL": f"Pre:PDCP SDU VOLUME, UL:{i}",
                   "PERC DL PRB UTIL": f"Pre:PERC DL PRB UTIL:{i}"})
    pivot_update_pre = pivot_update_pre.drop(columns=["Element3"])
    # Drop the 'Element3' column as it's no longer needed
    pivot_update_post = pivot_table_post.merge(filter_avg4G_post[
                                             ["Element3", "AVERAGE CQI", "DL_USER_THRPTFL18", "PDCP SDU VOLUME, DL",
                                              "PDCP SDU VOLUME, UL", "PERC DL PRB UTIL"]], left_on=i,
                                         right_on="Element3", how="left")
    pivot_update_post = pivot_update_post.rename(
        columns={"AVERAGE CQI": f"Post:AVERAGE CQI:{i}", "DL_USER_THRPTFL18": f"Post:DL_USER_THRPTFL18:{i}",
                 "PDCP SDU VOLUME, DL": f"Post:PDCP SDU VOLUME, DL:{i}",
                 "PDCP SDU VOLUME, UL": f"Post:PDCP SDU VOLUME, UL:{i}",
                 "PERC DL PRB UTIL": f"Post:PERC DL PRB UTIL:{i}"})
    pivot_update_post = pivot_update_post.drop(columns=["Element3"])
    # Update output_df to the new DataFrame
    pivot_table_pre = pivot_update_pre
    pivot_table_post = pivot_update_post
# Combine the specified headers into a new column
pivot_table_pre["Key"] = pivot_table_pre[bands].astype(str).agg('_'.join, axis=1)
pivot_table_post["Key"] = pivot_table_post[bands].astype(str).agg('_'.join, axis=1)
pivot_table_post.drop(columns=bands, inplace=True)
pivot_update_post.drop(columns="Site_ID_&_Sector", inplace=True)
combine_df = pivot_update_pre.merge(pivot_update_post, how="left", on="Key")
combine_df.drop(columns=["Key"], inplace=True)
# Loop through each band
for band in bands:
    # Extract "Pre" and "Post" headers for the band
    pre_headers = [col for col in combine_df.columns if col.startswith("Pre") and band in col]
    post_headers = [col.replace("Pre", "Post") for col in pre_headers]

    # Calculate percentage difference for each metric in the band
    for pre_col, post_col in zip(pre_headers, post_headers):
        metric_name = f"Delta:{pre_col.split(':')[1]}:{band}"
        combine_df[metric_name] = ((combine_df[post_col] - combine_df[pre_col]) / combine_df[pre_col]) * 100
    # Calculate share for each Post column
    for post_col in post_headers:
        share_name = f"%Share:{post_col.split(':')[1]}:{band}"
        combine_df[share_name] = (combine_df[post_col] / combine_df[post_headers].sum(axis=1)) * 100
# combine_df.to_csv(r"C:\Vodacom_TNZ\Pre&Post\\combine_df.csv", index=False)
# Reorder columns: Group by metric (e.g., A, B, C) and arrange Pre, Post, Diff
metrics = set(col.split(':')[1] for col in combine_df.columns if "Pre" in col)
# print(metrics)
ordered_columns = []
ordered_columns.append("Site_ID_&_Sector")
for i in bands:
    ordered_columns.append(i)
for metric in sorted(metrics):  # Order A, B, C...
    for band in bands:
        pre_col = f"Pre:{metric}:{band}"
        post_col = f"Post:{metric}:{band}"
        diff_col = f"Delta:{metric}:{band}"
        share_col = f"%Share:{metric}:{band}"
        if pre_col in combine_df.columns:
            ordered_columns.append(pre_col)
        if post_col in combine_df.columns:
            ordered_columns.append(post_col)
        if diff_col in combine_df.columns:
            ordered_columns.append(diff_col)
        if share_col in combine_df.columns:
            ordered_columns.append(share_col)

combine_df = combine_df[ordered_columns]




########---------2G color formatting---------------------------
# Function to color cells based on value, threshold, and operator
def color_cells_2G(col):
    # print(col)
    # Initialize a list for the styles
    styles = []
    for value in col:
        if value is not None and isinstance(value, (int, float)):
            value = float(value)
            try:
                threshold, operator = thresholds_2G[col.name]
                if operator == ">=":
                    styles.append('background-color: green' if value >= threshold else 'background-color: red')
                elif operator == "<=":
                    styles.append('background-color: green' if value <= threshold else 'background-color: red')
            except:
                styles.append('')
                pass
        else:
            styles.append('')  # No formatting for non-numeric cells
    return styles

# Dictionary to store thresholds and operators for each column


columns_to_remove = ["Date", "Primary_Key", "Element1", "Element2", "Element3"]
# List of columns to format
columns_to_format = [col for col in df_next_7_2G.columns if col not in columns_to_remove]
# Apply the formatting with the proper threshold and operator for each column
df_next_2G_7_color = df_next_7_2G.style.apply(color_cells_2G, subset=columns_to_format)

########---------3G color formatting---------------------------

# # Function to color cells based on value, threshold, and operator
def color_cells_3G(col):
    # print(col)
    # Initialize a list for the styles
    styles = []
    for value in col:
        if value is not None and isinstance(value, (int, float)):
            value = float(value)
            try:
                threshold, operator = thresholds_3G[col.name]
                if operator == ">=":
                    styles.append('background-color: green' if value >= threshold else 'background-color: red')
                elif operator == "<=":
                    styles.append('background-color: green' if value <= threshold else 'background-color: red')
            except:
                styles.append('')
                pass
        else:
            styles.append('')  # No formatting for non-numeric cells
    return styles


# Dictionary to store thresholds and operators for each column
columns_to_remove = ["Date", "Primary_Key", "Element1", "Element2", "Element3", "DN"]
# List of columns to format
columns_to_format = [col for col in df_next_7_3G.columns if col not in columns_to_remove]

# Apply the formatting with the proper threshold and operator for each column
df_next_3G_7_color = df_next_7_3G.style.apply(color_cells_3G, subset=columns_to_format)

#####------------4G color formatting-------------------------------------
def color_cells_4G(col):
    # print("col", col)
    # Initialize a list for the styles
    styles = []
    for value in col:
        # print("value", value)
        if value is not None and isinstance(value, (int, float)):
            value = float(value)
            try:
                threshold, operator = thresholds_4G[col.name]
                # print("threshold:", threshold, "operator:", operator)
                if operator == ">=":
                    # print(col, operator)
                    styles.append('background-color: green' if value >= threshold else 'background-color: red')
                elif operator == "<=":
                    # print(col, operator)
                    styles.append('background-color: green' if value <= threshold else 'background-color: red')
            except:
                styles.append('')
                pass
        else:
            styles.append('')  # No formatting for non-numeric cells
    return styles

    # Dictionary to store thresholds and operators for each column

columns_to_remove = ["Date", "Primary_Key", "Element1", "Element2", "Element3"]
# List of columns to format
columns_to_format = [col for col in df_next_7_4G.columns if col not in columns_to_remove]
# print(columns_to_format)
# Apply the formatting with the proper threshold and operator for each column
df_next_4G_7_color = df_next_7_4G.style.apply(color_cells_4G, subset=columns_to_format)














####-------------Saving Raw Data output-------------------------------------------
with pd.ExcelWriter(out_path+"output.xlsx", engine='xlsxwriter') as writer:
      # Write each DataFrame to a different sheet
      df_filtered_2G.to_excel(writer, sheet_name='2G', index=False)
      df_filtered_3G.to_excel(writer, sheet_name='3G', index=False)
      df_filtered_4G.to_excel(writer, sheet_name='4G', index=False)
      avg2G.to_excel(writer, sheet_name='2G_avg', index=False)
      avg3G.to_excel(writer, sheet_name='3G_avg', index=False)
      avg4G.to_excel(writer, sheet_name='4G_avg', index=False)
      diff_avg2G.to_excel(writer, sheet_name='diff_avg2G', index=False)
      diff_avg3G.to_excel(writer, sheet_name='diff_avg3G', index=False)
      diff_avg4G.to_excel(writer, sheet_name='diff_avg4G', index=False)
      combine_df.to_excel(writer, sheet_name='4G_additional_output', index=False)
      # summary_df.to_excel(writer, sheet_name='Summary_2G', index=False)

###-----------Saving Color Data output-------------------------------
#with pd.ExcelWriter(out_path + "next7days.xlsx", engine='xlsxwriter') as writer:
    # Write each DataFrame to a different sheet
#    df_next_2G_7_color.to_excel(writer, sheet_name='2G', index=False)
#    df_next_3G_7_color.to_excel(writer, sheet_name='3G', index=False)
#    df_next_4G_7_color.to_excel(writer, sheet_name='4G', index=False)

print("Data has been filtered....")