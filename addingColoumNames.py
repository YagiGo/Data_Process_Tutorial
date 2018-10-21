import pandas as pd
df = pd.read_csv('M_CM_TV_ADVERTISER.csv', header=None, encoding = "utf-8")
df.rename(columns={0: 'advertiser_code', 1: 'advertiser_name(kana)', 2: 'advertiser_name(jap)', 3: 'first_verision_date', 4: 'final_version_date'}, inplace=True)
df.to_csv('CM_TV_Advertiser.csv', index=False) # save to new csv file