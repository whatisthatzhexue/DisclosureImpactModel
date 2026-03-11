import re
import pandas as pd
import os
df=pd.read_csv('news_v1.csv')
root_output_dir='News'
companies = {"Berjaya": ["Berjaya Food Berhad","Berjaya Food Bhd", "Berjaya","BFood"],
             "F&N":["Fraser & Neave Holdings Berhad","Fraser & Neave Holdings Bhd","F&N","FNH","F & N"],
             "Power":["Power Root Berhad","Power Root Bhd","PWROOT","Power root","POWERROOT"],
             "QL":["QL Resources Berhad", "QL Resources Bhd","QL"]}

#Select targeted companies news
for comp, aliases in companies.items():
    pattern = re.compile("|".join([re.escape(alias) for alias in aliases]), re.IGNORECASE)
    df_comp=df[df['text'].apply(lambda x: bool(pattern.search(x)))]

    if len(df_comp) == 0:
        print(f"Not found「{comp}」news, Skip")
        continue

    comp_dir = os.path.join(root_output_dir, comp)
    os.makedirs(comp_dir, exist_ok=True)

    #Save
    output_path = os.path.join(comp_dir, f"{comp}_news.csv")
    df_comp.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f" Saved{len(df_comp)}「{comp}」 news → {output_path}")

print("Success!!!")