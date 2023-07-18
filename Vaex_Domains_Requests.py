import vaex
import os
import pprint

# Define file paths
requests_csv = "requests.csv"
requests_hdf5 = requests_csv + '.hdf5'

domains_csv = "domains.csv"
domains_hdf5 = domains_csv + '.hdf5'

export_csv_folder = "domains2022_2023.csv"

# Load the requests HDF5 if it exists, if not, convert CSV to HDF5 for requests to make this fast
df_requests = vaex.open(requests_hdf5) if os.path.isfile(requests_hdf5) else vaex.from_csv(requests_csv, convert=True, chunk_size=5_000_000, progress=True)

print("Requests loaded with success")

# Process 'Date' column in requests
df_requests['Date'] = df_requests['Date'].str.replace('Z', '').astype('datetime64[ns]')
df_requests_filtered_by_date = df_requests[df_requests['Date'].dt.year >= 2022]

print("Requests date filtered with success")

# Load the domains HDF5 if it exists, if not, convert CSV to HDF5 for domains to make this fast
df_domains = vaex.open(domains_hdf5) if os.path.isfile(domains_hdf5) else vaex.from_csv(domains_csv, convert=True, chunk_size=5_000_000, progress=True)

print("Domains loaded with success")

# Join the two dataframes on 'Request ID' column
#result_domains_filtered_by_date = df_domains.join(df_requests_filtered_by_date[['Request ID', 'Date']], on='Request ID', how='left')
result_domains_filtered_by_date = df_domains.join(df_requests_filtered_by_date[['Request ID', 'Date']], on='Request ID', how='inner')

print("Domains filtered by date with success")

# Count and print the number of rows in the resulting dataframe
print(f'Theres a total of {result_domains_filtered_by_date.count()} domains filtered by date.')

#result_domains_filtered_by_date.export_csv_arrow("arrow_" + export_csv_folder, progress=True)
result_domains_filtered_by_date.export_csv(export_csv_folder, progress=True)