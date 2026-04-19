#!/usr/bin/env python
# coding: utf-8

# In[6]:


#The Objective of this notebook is to restructure the data from the format Allison
#has the date records to the format needed to submit to MAPS


# In[7]:


#import nessary libaries
import pandas as pd
import matplotlib.pyplot as plt
import re
import string


# In[8]:


#Load your summary dataframe
df_2025 = pd.read_excel("summary sheets_2017-2025_03232026_proofed.xlsx")


# In[9]:


# Format the dates and add your missing base identifiers
df_2025.rename(columns={'Full Date Form': 'DATE'}, inplace=True)
df_2025['LOC'] = 'ABCD'      # Replace with your actual location code
df_2025['STATION'] = 'DEFG'  # Replace with your actual station code


# In[10]:


# Identify all the 'open' and 'close' column names
net_cols = [c for c in df_2025.columns if re.match(r'^N\d+b?\s*(open|close)(\s*a)?\s*$', c.strip())]
# Extract unique daily schedules
df_daily = df_2025[['LOC', 'STATION', 'DATE'] + net_cols].drop_duplicates()


# In[11]:


#Melt the rows
records = []

for _, row in df_daily.iterrows():
    loc, station, date = row['LOC'], row['STATION'], row['DATE']
    
    nets = set()
    for c in net_cols:
        match = re.search(r'^(N\d+b?)', c.strip())
        if match: nets.add(match.group(1))
            
    for net in nets:
        net_num = net.replace('N', '')
        
        # --- Handle First Session (open / close) ---
        open_col = next((c for c in net_cols if c.strip() == f"{net} open"), None)
        close_col = next((c for c in net_cols if c.strip() == f"{net} close"), None)
        if open_col and close_col:
            start_time, end_time = row[open_col], row[close_col]
            # Add row ONLY if times exist AND aren't 00:00:00
            if pd.notna(start_time) and pd.notna(end_time) and str(start_time).strip() != '00:00:00':
                records.append({'LOC': loc, 'STATION': station, 'DATE': date, 'NET': net_num, 'START': start_time, 'END': end_time})
                
        # --- Handle Second Session (open a / close a) ---
        open_a_col = next((c for c in net_cols if c.strip() == f"{net} open a"), None)
        close_a_col = next((c for c in net_cols if c.strip() == f"{net} close a"), None)
        if open_a_col and close_a_col:
            start_a, end_a = row[open_a_col], row[close_a_col]
            if pd.notna(start_a) and pd.notna(end_a) and str(start_a).strip() != '00:00:00':
                records.append({'LOC': loc, 'STATION': station, 'DATE': date, 'NET': net_num, 'START': start_a, 'END': end_a})


# In[12]:


# Explicitly define columns even if 'records' is empty to prevent KeyError
df_melted = pd.DataFrame(records, columns=['LOC', 'STATION', 'DATE', 'NET', 'START', 'END'])


# In[13]:


# Format Time and add LENGTH
def convert_time(t):
    if pd.isna(t): return t
    parts = str(t).split(':')
    if len(parts) >= 2: return int(parts[0] + parts[1])
    return t

df_melted['START'] = df_melted['START'].apply(convert_time)
df_melted['END'] = df_melted['END'].apply(convert_time)
df_melted['LENGTH'] = 1  


# In[14]:


# Calculate IP and SP for MAPS periods
# Convert DATE to datetime objects for logic check
df_melted['DATE_dt'] = pd.to_datetime(df_melted['DATE'])
df_melted['Year'] = df_melted['DATE_dt'].dt.year

def get_ip(date):
    if pd.isna(date): return pd.NA
    
    # Using (Month, Day) tuples for clean period ranges
    md = (date.month, date.day)
    
    if   (5, 1)  <= md <= (5, 10): return 1
    elif (5, 11) <= md <= (5, 20): return 2
    elif (5, 21) <= md <= (5, 30): return 3
    elif (5, 31) <= md <= (6, 9):  return 4
    elif (6, 10) <= md <= (6, 19): return 5
    elif (6, 20) <= md <= (6, 29): return 6
    elif (6, 30) <= md <= (7, 9):  return 7
    elif (7, 10) <= md <= (7, 19): return 8
    elif (7, 20) <= md <= (7, 29): return 9
    elif (7, 30) <= md <= (8, 8):  return 10
    
    return pd.NA

df_melted['IP'] = df_melted['DATE_dt'].apply(get_ip)

valid_ip_mask = df_melted['IP'].notna()
if not df_melted[valid_ip_mask].empty:
    # Calculate Station Pass (SP) - rank dates within the specific Location, Year, and IP
    df_melted.loc[valid_ip_mask, 'Date_Rank'] = df_melted[valid_ip_mask].groupby(['LOC', 'Year', 'IP'])['DATE_dt'].rank(method='dense')
    rank_to_letter = {float(i+1): letter for i, letter in enumerate(string.ascii_uppercase)}
    df_melted['SP'] = df_melted['Date_Rank'].map(rank_to_letter)
else:
    df_melted['SP'] = pd.NA

df_melted['IP'] = df_melted['IP'].fillna('')
df_melted['SP'] = df_melted['SP'].fillna('')


# In[15]:


# Final Sorting and Column Ordering
final_cols = ['LOC', 'STATION', 'DATE', 'IP', 'SP', 'NET', 'LENGTH', 'START', 'END']

# Sort safely so 'NET 10' comes after 'NET 9'
df_final = df_melted.sort_values(
    by=['LOC', 'DATE_dt', 'NET'], 
    key=lambda col: col.map(lambda x: int(re.search(r'\d+', str(x)).group()) if re.search(r'\d+', str(x)) else 0) if col.name == 'NET' else col
)[final_cols]

print("Rows created:", len(df_final))
print(df_final.head(10))


# In[16]:


df_final.to_csv("Map_Dates.csv", index=False)

