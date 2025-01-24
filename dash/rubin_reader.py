import pyarrow.parquet as pq
import astropy.units as u
import pandas as pd
import numpy as np

from hats_import.catalog.file_readers import ParquetReader

class RubinParquetReader(ParquetReader):

    def read(self, input_file, read_columns=None):
        """Reader for the specifics of Rubin parquet files."""
        if read_columns is not None:
            # Mapping stage, so we only need ra, dec set in read_columns
            kwargs = {"columns": read_columns}
            table = pq.read_table(input_file, **kwargs).to_pandas()
        else:
            # Other stages need all pre-selected columns
            kwargs = {"columns": self.column_names} if self.column_names else {}
            table = pq.read_table(input_file, **kwargs).to_pandas()
            table = self.process_rubin_table(table)
        yield table
        
    def process_rubin_table(self, table):
        dataset_type = self.kwargs["dataset_type"]
        if dataset_type == "diaSourceTable_tract":
            return self.process_diaSourceTable_tract(table)
        elif dataset_type == "forcedSourceOnDiaObjectTable":
            return self.process_forcedSourceOnDiaObjectTable(table)
        elif dataset_type == "objectTable":
            return self.process_objectTable(table)
        elif dataset_type == "sourceTable":
            return self.process_sourceTable(table)
        elif dataset_type == "forcedSourceTable":
            return self.process_forcedSourceTable(table)
        return table

    def process_diaSourceTable_tract(self, table):
        flux_col_prefixes = []
        for flux_name in ["psf","science"]:
            if f"{flux_name}Flux" in table.columns:
                flux_col_prefixes.append(flux_name)
        if len(flux_col_prefixes) > 0:
            table = append_mag_and_magerr(table, flux_col_prefixes)
        return table
    
    def process_forcedSourceOnDiaObjectTable(self, table):
        if "psfFlux" in table.columns:
            table = append_mag_and_magerr(table, flux_col_prefixes=["psf"])
        return table
        
    def process_objectTable(self, table):
        flux_col_prefixes = []
        for band in list("ugrizy"):
            for flux_name in ["psf","kron"]:
                band_col = f"{band}_{flux_name}"
                if f"{band_col}Flux" in table.columns:
                    flux_col_prefixes.append(band_col)
        if len(flux_col_prefixes) > 0:
            table = append_mag_and_magerr(table, flux_col_prefixes)    
        return table

    def process_sourceTable(self, table):
        if "psfFlux" in table.columns:
            table = append_mag_and_magerr(table, flux_col_prefixes=["psf"])
        return table
 
    def process_forcedSourceTable(self, table):
        # Add the missing MJDs
        if "visit" in table.columns:
            visit_map = self.kwargs["visit_map"]
            mjds = list(map(lambda x: visit_map.get(x, 0.0), table["visit"]))
            table["midpointMJDTai"] = pd.Series(mjds, index=table.index)
        if "psfFlux" in table.columns:
            table = append_mag_and_magerr(table, flux_col_prefixes=["psf"])
        return table


def append_mag_and_magerr(table, flux_col_prefixes):
    """Calculate magnitudes and their errors for flux columns."""
    mag_cols = {}
    
    for prefix in flux_col_prefixes:
        # Magnitude
        flux = table[f"{prefix}Flux"]
        mag = u.nJy.to(u.ABmag, flux)
        mag_cols[f"{prefix}Mag"] = mag

        # Magnitude error, if flux error exists
        fluxErr_col = f"{prefix}FluxErr"
        if fluxErr_col in table.columns:
            fluxErr = table[fluxErr_col]
            upper_mag = u.nJy.to(u.ABmag, flux+fluxErr)
            lower_mag = u.nJy.to(u.ABmag, flux-fluxErr)
            magErr = -(upper_mag-lower_mag)/2
            mag_cols[f"{prefix}MagErr"] = magErr
        
    mag_table = pd.DataFrame(mag_cols, dtype=np.float64, index=table.index)
    return pd.concat([table, mag_table], axis=1)
