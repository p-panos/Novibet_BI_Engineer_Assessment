# This script processes casino data from multiple CSV files, cleans and transforms the data.

#import libraries
import pandas as pd
from datetime import date

#Set path to local file folder
path = 'C:\\Users\\plata\\Downloads\\novibet_eng\\files'


#Read files and parse to dataframes
casinodaily = pd.read_csv(path + '\\casinodaily.csv', sep=',')
users = pd.read_csv(path + '\\users.csv', sep=',')
casinoproviders = pd.read_csv(path + '\\casinoproviders.csv', sep=',')
currencyrates = pd.read_csv(path + '\\currencyrates.csv', sep=',')
casinomanufacturers = pd.read_csv(path + '\\casinomanufacturers.csv', skiprows=1, header = None)



#Cleaning casinomanufacturers dataframe

# Assign correct column names
casinomanufacturers.columns = [
    "CasinoManufacturerId", "CasinoManufacturerName",
    "FromDate", "ToDate", "LatestFlag"
]

# Strip whitespace from string columns
casinomanufacturers = casinomanufacturers.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Convert date columns to datetime
casinomanufacturers["FromDate"] = pd.to_datetime(casinomanufacturers["FromDate"], errors='coerce')
casinomanufacturers["ToDate"] = pd.to_datetime(casinomanufacturers["ToDate"], errors='coerce')

# Identify rows where 'CasinoManufacturerId' contains a comma (bad rows)
bad_rows_mask = casinomanufacturers['CasinoManufacturerId'].astype(str).str.contains(',')

# Split those rows by comma and expand into columns
split_rows = casinomanufacturers[bad_rows_mask]['CasinoManufacturerId'].str.split(',', expand=True)

# If there are enough columns, assign them to the correct columns
for i, col in enumerate(casinomanufacturers.columns):
    if i < split_rows.shape[1]:
        casinomanufacturers.loc[bad_rows_mask, col] = split_rows[i]


# Convert 'CasinoManufacturerId' to numeric, coercing errors
casinomanufacturers["LatestFlag"] = casinomanufacturers["LatestFlag"].replace({"1": 1, "0": 0})

casinomanufacturers["LatestFlag"] = pd.to_numeric(casinomanufacturers["LatestFlag"], errors="coerce")

#Filter casinomanufacturers to keep only the latest entries
casinomanufacturers_latest = casinomanufacturers[casinomanufacturers["LatestFlag"] == 1]


# Remove double quotes from all string columns
for col in casinomanufacturers.select_dtypes(include='object').columns:
    casinomanufacturers[col] = casinomanufacturers[col].str.replace('"', '', regex=False).str.strip()


#Remove rows with non valid BirthDate from dataframe users
users['BirthDate_year'] = users['BirthDate'].str[:4]
users.BirthDate_year = pd.to_numeric(users.BirthDate_year, errors='coerce')
users = users[users.BirthDate_year < 2025]


# Calculate years of age and assign create the respective buckets
users['BirthDate'] = pd.to_datetime(users['BirthDate'])
users['Age'] = ((pd.to_datetime(date.today()) -  users['BirthDate']).dt.days)/365
users['Age'] = round(users['Age'],0)

# Assign the users to the repective age groups
users.loc[users['Age']<18, 'Age_group'] = 'Under 18'
users.loc[users['Age'].between(21,26), 'Age_group'] = '21-26'
users.loc[users['Age'].between(27,32), 'Age_group'] = '27-32'
users.loc[users['Age'].between(33,40), 'Age_group'] = '33-40'
users.loc[users['Age'].between(41,50), 'Age_group'] = '41-50'
users.loc[users['Age']>50, 'Age_group'] = '50+'

#Clean the VIPStatus column in users dataframe
users['VIPStatus'] = users['VIPStatus'].str.replace(' ', '', regex=False).str.title()

#Clean the CasinoManufacturerName column in casinomanufacturers dataframe
casinomanufacturers['CasinoManufacturerName'] = casinomanufacturers['CasinoManufacturerName'].str.replace(' ', '', regex=False).str.title()

#Clean the casinoproviders dataframe. CasinoProviderId values should be unique. Need to align the name of providers.
casinoproviders['CasinoProviderName'] = casinoproviders['CasinoProviderName'].replace("MicroGaming", "GamesGlobal")
casinoproviders['CasinoProviderName'] = casinoproviders['CasinoProviderName'].replace("Nyx", "LightAndWonder")
casinoproviders['CasinoProviderName'] = casinoproviders['CasinoProviderName'].replace("ESA", "ESAGaming")
casinoproviders['CasinoProviderName'] = casinoproviders['CasinoProviderName'].replace("EGTGaming", "EGT")


# Perform inner join between users and casinodaily dataframes
users_casinodaily = users.merge(casinodaily, left_on="user_id", right_on="UserID", how="inner")

#Inner join the above result with casinomanufacturers dataframe. CasinomanufactererId is a string in casinomanufacturers, convert it to int64.
casinomanufacturers['CasinoManufacturerId'] = casinomanufacturers['CasinoManufacturerId'].astype('int64')

#Perform inner join
users_casinodaily_manufacturers = users_casinodaily.merge(
    casinomanufacturers,
    left_on="CasinoManufacturerId",
    right_on="CasinoManufacturerId",
    how="inner"
)


#Inner join the above result with casinoproviders dataframe.
users_casinodaily_manufacturers_providers = users_casinodaily_manufacturers.merge(
    casinoproviders,
    left_on="CasinoProviderId",
    right_on="CasinoProviderId",
    how="inner"
)


#Inner join the above result with currencyrates dataframe. This will give us the consolidated dataframe with all the necessary columns.
casino_consolidated = users_casinodaily_manufacturers_providers.merge(
    currencyrates,
    left_on="Date",
    right_on="Date",
    how="inner"
)


#Convert the GGR and Returns columns to EUR and store the values in new columns.
casino_consolidated["GGR_EUR"] = casino_consolidated["GGR"] * casino_consolidated["EuroRate"]
casino_consolidated["Returns_EUR"] = casino_consolidated["Returns"] * casino_consolidated["EuroRate"]


# Perform aggregations on the requested levels and create the final dataframe.
agg_levels = ["Date", "Country", "Sex", "Age_group", "VIPStatus", "CasinoManufacturerName", "CasinoProviderName"]
casino_final = casino_consolidated.groupby(agg_levels)[["GGR_EUR", "Returns_EUR"]].sum().reset_index()



# Export the final dataframe to an Excel file.
casino_final.to_excel(path + '\\casino_final.xlsx', index=False)


