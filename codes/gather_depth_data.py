from astropy.table import Table
import numpy as np
import pandas as pd
import os
from multiprocessing import Pool, Manager
from tqdm import tqdm  # For progress tracking (optional)
from logpool import control  # Assuming logpool is compatible with multiprocessing

# Directory and data setup
dual_cats = os.listdir("/storage/splus/Catalogues/iDR5/idr5/dual/")
bands = ["J0378", "J0395", "J0410", "J0430", "J0515", "J0660", "J0861", "i", "r", "u", "z", "g"]
snr_values = [3, 5, 10, 50]

# Function to process each file
def process_file(file):
    control.info(f"Processing file {file}")
    results = []
    try:
        tab = Table.read(f"/storage/splus/Catalogues/iDR5/idr5/dual/{file}")

        for band in bands:
            for snr_value in snr_values:
                # Filter data based on SNR
                if f"s2n_{band}_petro" in tab.colnames and f"{band}_petro" in tab.colnames:
                    snr_selection = tab[tab[f"s2n_{band}_petro"] > snr_value]
                    data = snr_selection[f"{band}_petro"]

                    # Remove NaNs, infs, and outliers
                    data = data[np.isfinite(data)]
                    data = data[(data > 16) & (data < 24)]

                    if len(data) > 0:
                        # Create histogram and find peak
                        counts, bin_edges = np.histogram(data, bins=100, density=True)
                        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
                        max_index = np.argmax(counts)
                        peak_value = bin_centers[max_index]

                        # Append result
                        results.append({
                            'field': file,
                            'band': band,
                            'peak': peak_value,
                            'snr_value': snr_value
                        })
        
    except Exception as e:
        print(f"Error processing {file}: {e}")
    return results

if __name__ == "__main__":
    # Use multiprocessing to process files in parallel
    with Pool(processes=os.cpu_count()) as pool:
        all_results = list(tqdm(pool.imap(process_file, dual_cats), total=len(dual_cats)))

    # Flatten the list of results
    flat_results = [item for sublist in all_results for item in sublist]

    # Create DataFrame
    df = pd.DataFrame(flat_results)
    
    # Save to CSV
    df.to_csv("../data/dual_depth_peaks.csv", index=False)

    # Display the first few rows
    print(df.head())