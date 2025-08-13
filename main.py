from sheet import dataframe, upload_to_sheet
from pkg_details import get_line_item_ids_by_name
from googleads import ad_manager
from dotenv import load_dotenv
import os
import pandas as pd
load_dotenv()
OLD_GAM = os.environ["OLD_GAM"]
NEW_GAM = os.environ["NEW_GAM"]
df = dataframe()


pkg_names = df["lineitem_name"].unique().tolist()


old_client = ad_manager.AdManagerClient.LoadFromStorage(OLD_GAM)
new_client = ad_manager.AdManagerClient.LoadFromStorage(NEW_GAM)

old_gam = get_line_item_ids_by_name(old_client, pkg_names)
new_gam = get_line_item_ids_by_name(new_client, pkg_names)
merged_output = old_gam + new_gam

def clean_line_item_name(name):
        if pd.isnull(name):
            return name
        while len(name) >= 5 and not name[-5:].isdigit():
            name = name[:-1]
        return name
new_df = pd.DataFrame(merged_output)


new_df['line_item_name'] = new_df['line_item_name'].apply(clean_line_item_name)

new_df = new_df.groupby('line_item_name')[['impression','psbk', 'NewsPoint']].sum().reset_index()

new_df.to_csv("line_item_name.csv",index=False, encoding="utf-8")
upload_df = pd.read_csv("line_item_name.csv")
upload_to_sheet(upload_df)