import numpy as np
import os
import pandas as pd
from astropy.table import Table
import shutil

class TargetData:
    def __init__(self, master_path="merged_table.csv"):
        self.master_path = master_path
        self.data = pd.DataFrame()
        self._load_master()
        #possible names for columns, need to create standardized format for data files later
        self.column_names = {
            'name': ['ID', 'Name', 'name', 'Target Name', 'target name'],
            'magnitude': ['Mag', 'mag', 'Magnitude', 'magnitude', 'brightness'],
            'ra': ['RA', 'ra', 'right ascencion', 'Right Ascencion', 'Ra'],
            'dec': ['DEC', 'dec', 'Dec', 'Declination', 'declination']
            'v_mag': ['V', 'vmag', 'V_mag', 'v_band', 'Vmag'],
            'b_mag': ['B', 'bmag', 'B_mag', 'b_band', 'Bmag'],
            'r_mag': ['R', 'rmag', 'R_mag', 'r_band', 'Rmag'],
            'g_mag': ['G', 'gmag', 'G_mag', 'g_band', 'Gmag'],
            'i_mag': ['I', 'imag', 'I_mag', 'i_band', 'Imag']
        }

    def _load_master(self):
        if os.path.exists(self.master_path):
            print(f"Loading from {self.master_path}...")
            self.data = pd.read_csv(self.master_path)
            print(f"Successfully loaded {len(self.data)} targets.")
        else:
            print("No existing master list found")

    def save_merge(self):
        if not self.data.empty:
            self.data.to_csv(self.master_path, index=False)
            print(f"Saved {len(self.data)} targets to {self.master_path}.")
        else:
            print("No data, nothing to save")

    def read_file(self, filepath):
        try:
            _, file_extension = os.path.splitext(filepath)
            temp_df = None
            #code needs to be able to handle: csv, dat, txt, and fits
            if file_extension.lower() == '.csv':
                temp_df = pd.read_csv(filepath, engine='python')
            elif file_extension.lower() in ['.txt', '.dat']:
                temp_df = pd.read_csv(filepath, sep=r'\s+', engine='python')
            elif file_extension.lower() == '.fits':
                fits_table = Table.read(filepath)
                temp_df = fits_table.to_pandas()
            else:
                print(f"Unsupported file type: {file_extension}")
                return False

            if temp_df is not None:
                print(f"Read {len(temp_df)} entries from {os.path.basename(filepath)}.")
                temp_df.columns = [c.lower().strip() for c in temp_df.columns]

                if not all(col in temp_df.columns for col in ['ra', 'dec', 'name']):
                    print("Missing 'ra', 'dec', or 'name'.")
                    return False