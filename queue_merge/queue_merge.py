import numpy as np
import os
import pandas as pd
import logging
from astropy.table import Table
import shutil
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
from datetime import datetime, timezone
log = logging.getLogger(__name__)

def _normalize_label(s):
    return str(s).strip().lower()

#standardizing column names
def standardization_map(lists_map, overrides=None):
    flat = {}
    for standard, variations in lists_map.items():
        std_key = _normalize_label(standard)
        for label in [standard, *variations]:
            k = _normalize_label(label)
            if k in flat and flat[k] !=std_key:
                log.warning("Header conflict for '%s': keeping '%s', ignoring '%s'", k, flat[k], std_key)
                continue
            flat[k] = std_key
    if overrides:
        for k, v in overrides.items():
            flat[_normalize_label(k)] = _normalize_label(v)
    return flat

class TargetData:
    def __init__(self, master_path="merged_table.csv"):
        self.master_path = master_path
        self.data = pd.DataFrame()
        self._load_master()
        #possible names for columns, need to create standardized format for data files later
        self.column_names = {
            'name': ['ID', 'Name', 'name', 'Target Name', 'target name'],
            'magnitude': ['Mag', 'mag', 'Magnitude', 'magnitude', 'brightness'],
            "ra": ["RA", "ra", "Right Ascension", "Right Ascencion", "RA_J2000", "RA(deg)"],
            "dec": ["DEC", "dec", "Dec", "Declination", "declination", "DEC_J2000", "DEC(deg)"],
            'v_mag': ['V', 'vmag', 'V_mag', 'v_band', 'Vmag', 'visual'],
            'b_mag': ['B', 'bmag', 'B_mag', 'b_band', 'Bmag', 'blue'],
            'r_mag': ['R', 'rmag', 'R_mag', 'r_band', 'Rmag', 'red'],
            'g_mag': ['G', 'gmag', 'G_mag', 'g_band', 'Gmag', 'green'],
            'i_mag': ['I', 'imag', 'I_mag', 'i_band', 'Imag', 'infrared']
        }

        self.standardization_map = standardization_map(
            self.column_names,
            overrides={"ra": "ra", "dec": "dec", "mag": "magnitude"}
        )
    
    def standardize_columns(self, df):
        new_cols = {}
        for c in df.columns:
            k = _normalize_label(c)
            new_cols[c] = self.standardization_map.get(k, k)
        out = df.rename(columns=new_cols)
        required = ["name", "ra", "dec"]
        missing = [c for c in required if c not in out.columns]
        if missing:
            raise ValueError(
                f"Missing required columns {missing}. Got {list(out.columns)}"
            )

        if "magnitude" not in out.columns:
            out["magnitude"] = pd.NA

        return out


    def _load_master(self):
        if os.path.exists(self.master_path):
            log.info("Loading from %s...", self.master_path)
            self.data = pd.read_csv(self.master_path)
            log.info("Successfully loaded %d targets.", len(self.data))
        else:
            log.info("No existing master list found")

    def save_merge(self):
        if not self.data.empty:
            self.data.drop_duplicates(subset=['name'], keep='first', inplace=True)
            self.data.to_csv(self.master_path, index=False)
            log.info("Saved %d targets to %s.", len(self.data), self.master_path)
        else:
            log.info("No data, nothing to save")

    def read_file(self, filepath):
        try:
            _, file_extension = os.path.splitext(filepath)
            temp_df = None
            #code needs to be able to handle: csv, dat, txt, and fits
            if file_extension.lower() == '.csv':
                temp_df = pd.read_csv(filepath)
            elif file_extension.lower() in ['.txt', '.dat']:
                temp_df = pd.read_csv(filepath, sep=r'\s+', engine='python')
            elif file_extension.lower() == '.fits':
                fits_table = Table.read(filepath)
                temp_df = fits_table.to_pandas()
            else:
                log.info("Unsupported file type %s", file_extension)
                return False

            if temp_df is not None:
                log.info("Read %d entries from %s", len(temp_df), os.path.basename(filepath))
                temp_df = self.standardize_columns(temp_df)
                self.data = pd.concat([self.data, temp_df], ignore_index=True)
                log.info("Merged %d rows from %s (total=%d)", len(temp_df), os.path.basename(filepath), len(self.data))
                return True
            return False

        except Exception as e:
            log.exception("An error occurred while reading %s", filepath)
            return False


def process_directory(input_dir):
    input_path = Path(input_dir).resolve()
    if not input_path.is_dir():
        log.error("Not a directory: %s", input_dir)
        return False

    td = TargetData()
    td.master_path = str(input_path / "merged_table.csv")
    td._load_master()

    supported = {".csv", ".txt", ".dat", ".fits"}
    files = [p for p in input_path.iterdir() if p.is_file() and p.suffix.lower() in supported]
    if not files:
        log.info("No supported files found in %s", input_path)
        return False

    processed_dir = input_path / "processed"
    processed_dir.mkdir(exist_ok=True)

    merged_count = 0
    for p in files:
        pre_len = len(td.data)
        ok = td.read_file(str(p))
        if not ok:
            log.info("Skipped (read failed): %s", p.name)
            continue

        rows_added = len(td.data) - pre_len
        if rows_added > 0:
            now = datetime.now(timezone.utc).isoformat()
            td.data.loc[pre_len:pre_len + rows_added - 1, "updated_at"] = now
            td.data.loc[pre_len:pre_len + rows_added - 1, "source_file"] = p.name


            try:
                shutil.move(str(p), str(processed_dir / p.name))
                log.info("Merged %d row(s) from %s → moved to processed/", rows_added, p.name)
            except Exception:
                log.exception("Could not move %s into %s", p.name, processed_dir)
            merged_count += 1
        else:
            log.info("No new rows added from %s (possible duplicates). Not moving.", p.name)

    if merged_count == 0:
        log.info("No files contributed new rows. Nothing to save.")
        return False

    td.save_merge()
    log.info("Directory merge complete: %d file(s) contributed new rows.", merged_count)
    return True

def launch_gui():

    root = tk.Tk()
    root.title("Sky Queue")
    root.geometry("360x140")

    status = tk.StringVar(value="Choose a folder (e.g., data or tests)")

    def choose_and_run():
        folder = filedialog.askdirectory(title="Select folder (data or tests)")
        if not folder:
            return
        ok = process_directory(folder)
        if ok:
            status.set(f"Done ✓  Saved merged_table.csv in: {Path(folder).name}")
            messagebox.showinfo("Sky Queue", f"Merged & saved in:\n{folder}")
        else:
            status.set("No changes. See logs for details.")
            messagebox.showwarning("Sky Queue", "No files merged. Check logs/inputs.")

    tk.Button(root, text="Select folder & merge…", width=24, command=choose_and_run).pack(pady=12)
    tk.Label(root, textvariable=status).pack(pady=4)

    root.mainloop()
