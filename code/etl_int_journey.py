import pandas as pd
import csv

## USE THIS QUERY TO GET DATA ##
# Get fct_journey data like this:
# select
#   f.id as ID,
#   f.shop_key as SHOP_KEY,
#   f.zone_key as ZONE_KEY,
#   f.visitor_id as VISITOR_ID,
#   f.date_id as DATE_ID,
#   f.time_id as TIME_ID,
#   d.fulldate as DATE,
#   t.fulltime as TIME
# from fct_journey f
# left join
#     dim_date d on d.dateid = f.date_id
# left join
#     dim_time t on t.timeid = f.time_id
# order by d.fulldate, t.fulltime;




fct = pd.read_csv('../etl_data/staging/fct_journey_data_test.csv', header=0)
fct = fct.sort_values(by=["DATE", "TIME"])
fct["ZONE_ENTRY_ID"] = ""
fct["ZONE_EXIT_ID"] = ""
fct["SHOP_ENTRY_ID"] = ""
fct["SHOP_EXIT_ID"] = ""

visitor_ids = fct["VISITOR_ID"].unique()
fct_processed = []


for v in visitor_ids:
    sub = fct[fct["VISITOR_ID"] == v]
    sub = sub.reset_index(drop=True)

    # sub = fct[fct["VISITOR_ID"] == 18]
    # sub = sub.reset_index(drop=True)
    # sub.loc[[1, 2, 3], "ZONE_KEY"] = 'Z5_5'
    # sub.loc[[5, 7], "ZONE_KEY"] = 'Z4_4'
    # sub.loc[[14, 15], "ZONE_KEY"] = 'Z5_5'

    for i in sub.index:
        # make sure the index is valid since we are using i+1 lookahead
        if i < sub.last_valid_index():
            current_date = sub.loc[i, "DATE"]
            next_date = sub.loc[i+1, "DATE"]   
            previous_zone = sub.loc[i-1 if i > 0 else i, "ZONE_KEY"] ; current_zone = sub.loc[i, "ZONE_KEY"]; next_zone = sub.loc[i+1, "ZONE_KEY"]
            previous_shop = sub.loc[i-1 if i > 0 else i, "SHOP_KEY"] ; current_shop = sub.loc[i, "SHOP_KEY"] ; next_shop = sub.loc[i+1, "SHOP_KEY"]
            shop_key_null = sub["SHOP_KEY"].isna()

            if i > 0:
                if next_date == current_date:        
                    if previous_zone == current_zone: 
                        sub.loc[i, "ZONE_ENTRY_ID"] = sub.loc[i-1, "ZONE_ENTRY_ID"]
                        
                        if shop_key_null[i] == False:
                            if shop_key_null[i-1] == False and previous_shop == current_shop:  
                                sub.loc[i, "SHOP_ENTRY_ID"] = sub.loc[i-1, "SHOP_ENTRY_ID"]
                            else: 
                                sub.loc[i, "SHOP_ENTRY_ID"] = sub.loc[i, "TIME_ID"]
                    else:
                        sub.loc[i, "ZONE_ENTRY_ID"] = sub.loc[i, "TIME_ID"]
                        if shop_key_null[i] == False:
                            sub.loc[i, "SHOP_ENTRY_ID"] = sub.loc[i, "TIME_ID"]

                    if next_zone == current_zone:
                        sub.loc[i, "ZONE_EXIT_ID"] = ""
                        
                        if shop_key_null[i] == False:
                            if shop_key_null[i+1] == False and next_shop == current_shop:
                                sub.loc[i, "SHOP_EXIT_ID"] = ""
                            else:
                                sub.loc[i, "SHOP_EXIT_ID"] = sub.loc[i+1, "TIME_ID"]
                    else:
                        sub.loc[i, "ZONE_EXIT_ID"] = sub.loc[i+1, "TIME_ID"]

                        if shop_key_null[i] == False:
                            sub.loc[i, "SHOP_EXIT_ID"] = sub.loc[i+1, "TIME_ID"]    
                else:
                    if previous_zone == current_zone: 
                        sub.loc[i, "ZONE_ENTRY_ID"] = sub.loc[i-1, "ZONE_ENTRY_ID"]
                        
                        if shop_key_null[i] == False:
                            if shop_key_null[i-1] == False and previous_shop == current_shop:  
                                sub.loc[i, "SHOP_ENTRY_ID"] = sub.loc[i-1, "SHOP_ENTRY_ID"]
                            else:
                                sub.loc[i, "SHOP_ENTRY_ID"] = sub.loc[i, "TIME_ID"]
                            sub.loc[i, "SHOP_EXIT_ID"] = sub.loc[i, "TIME_ID"] 
                    else:
                        sub.loc[i, "ZONE_ENTRY_ID"] = sub.loc[i, "TIME_ID"]
                        sub.loc[i, "SHOP_ENTRY_ID"] = sub.loc[i, "TIME_ID"]

                    sub.loc[i, "ZONE_EXIT_ID"] = sub.loc[i, "TIME_ID"]
                    sub.loc[i, "SHOP_EXIT_ID"] = sub.loc[i, "TIME_ID"]
            else:
                sub.loc[i, "ZONE_ENTRY_ID"] = sub.loc[i, "TIME_ID"]
                if shop_key_null[i] == False:
                    sub.loc[i, "SHOP_ENTRY_ID"] = sub.loc[i, "TIME_ID"]
                
                if next_zone == current_zone:  
                    sub.loc[i, "ZONE_EXIT_ID"] = ""
                    
                    if shop_key_null[i+1] == False and next_shop == current_shop:
                        sub.loc[i, "SHOP_EXIT_ID"] = ""
                else:
                    sub.loc[i, "ZONE_EXIT_ID"] = sub.loc[i+1, "TIME_ID"]
                    if shop_key_null[i] == False: 
                        sub.loc[i, "SHOP_EXIT_ID"] = sub.loc[i+1, "TIME_ID"]
        else:
            sub = sub.drop(i)
    fct_processed.append(sub)

# Test print()
# print(sub.loc[0:20, ["SHOP_KEY", "ZONE_KEY", "DATE", "TIME", "DATE_ID", "TIME_ID", "ZONE_ENTRY_ID", "ZONE_EXIT_ID", "SHOP_ENTRY_ID", "SHOP_EXIT_ID"]])

fct_final = pd.concat(fct_processed)
fct_final = fct_final.sort_values(by=["DATE", "TIME"])
fct_final = fct_final.drop(["DATE", "TIME"], axis=1)
fct_final = fct_final.rename(columns={"TIME_ID": "TIMESTAMP_ID", "ZONE_ENTRY_ID": "ZONE_ENTRY_TIME_ID", "ZONE_EXIT_ID": "ZONE_EXIT_TIME_ID", "SHOP_ENTRY_ID": "SHOP_ENTRY_TIME_ID", "SHOP_EXIT_ID": "SHOP_EXIT_TIME_ID"})

fct_final.to_csv('../etl_data/processed/int_journey_dwell_time_etl_v2.csv', ",", index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC)



