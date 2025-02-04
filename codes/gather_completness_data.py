from astropy.io import fits
from astropy.table import Table
import pandas as pd
import os
from multiprocessing import Pool, Manager
from logpool import control  # Assuming logpool is compatible with multiprocessing

# File directory and bands setup
dual_cats = os.listdir("/storage/splus/Catalogues/iDR5/idr5/dual/")
bands = ["J0378", "J0395", "J0410", "J0430", "J0515", "J0660", "J0861", "i", "r", "u", "z", "g"]
limits = [14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]

# Initialize shared dictionary for results using Manager
manager = Manager()
final_results = manager.dict({limit: manager.dict({band: 0 for band in bands}) for limit in limits})

def sum_to_final_results(args):
    tablename, num = args
    control.info(f"Processing file {num}: {tablename}")

    # Read FITS file
    tab = Table.read(f"/storage/splus/Catalogues/iDR5/idr5/dual/{tablename}")

    local_results = {limit: {band: 0 for band in bands} for limit in limits}

    # Summing logic
    control.info("Summing")
    for band in bands:
        for limit in limits:
            lower = limit - 0.5
            upper = limit + 0.5
            
            length = len(
                tab[(tab["r_petro"] > lower) & 
                    (tab["r_petro"] <= upper) &
                    (tab[f"{band}_petro"] != 99)]
            )
            local_results[limit][band] += length

    # Updating shared dictionary
    for limit in limits:
        for band in bands:
            final_results[limit][band] += local_results[limit][band]

    control.info(f"Done processing {tablename}")

if __name__ == "__main__":
    # Use Pool for multiprocessing
    with Pool(processes=os.cpu_count()) as pool:
        pool.map(sum_to_final_results, [(dual_cat, idx) for idx, dual_cat in enumerate(dual_cats)])

    # Prepare DataFrame from final_results
    rows = []
    for limit_range, bands_data in final_results.items():
        for band, count in bands_data.items():
            rows.append({'band': band, 'limit_range': float(limit_range), 'count': count, 'type': 'dual'})

    # Create and save DataFrame
    df = pd.DataFrame(rows)
    df.to_csv("../data/dual.csv", index=False)