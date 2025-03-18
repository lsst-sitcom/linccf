from hats_import.catalog.file_readers import ParquetReader

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


class DimensionParquetReader(ParquetReader):

    PRIMARY_FLAG_COL = "detect_isPrimary"

    def __init__(
        self, chunksize=500_000, column_names=None, filter_non_primary=False, **kwargs
    ):
        self.chunksize = chunksize
        self.column_names = column_names
        self.filter_non_primary = filter_non_primary
        self.kwargs = kwargs

    def read(self, input_file, read_columns=None):
        self.regular_file_exists(input_file, **self.kwargs)

        columns = read_columns or self.column_names
        # If `filter_non_primary` is set, we need to load the primary column
        if self.filter_non_primary and columns and self.PRIMARY_FLAG_COL not in columns:
            columns.append(self.PRIMARY_FLAG_COL)

        batch_files = pd.read_csv(input_file)
        added_columns = set(batch_files.columns) - set(["path"])

        batch_size = 0
        batch_tables = []

        for _, row in batch_files.iterrows():
            parquet_file = pq.ParquetFile(row["path"], **self.kwargs)
            for smaller_table in parquet_file.iter_batches(
                batch_size=self.chunksize, columns=columns
            ):
                table = pa.Table.from_batches([smaller_table])
                table = table.replace_schema_metadata()
                table = self._filter_rows(table)

                if read_columns is None:
                    ## splitting stage - add in dimension columns
                    for column in added_columns:
                        if column not in table.column_names:
                            table = table.append_column(
                                column, [np.full(len(table), fill_value=row[column])]
                            )
                if batch_size + len(table) >= self.chunksize:
                    # We've hit our chunksize, send the batch off to the task.
                    if len(batch_tables) == 0:
                        yield table
                        batch_size = 0
                    else:
                        yield pa.concat_tables(batch_tables)
                        batch_tables = []
                        batch_tables.append(table)
                        batch_size = len(table)
                else:
                    batch_tables.append(table)
                    batch_size += len(table)

        if len(batch_tables) > 0:
            yield pa.concat_tables(batch_tables)

    def _filter_rows(self, table):
        """Filter invalid and/or undesired rows.

        - Rows with invalid/undefined ra/dec coordinates
        - Rows belonging to non-primary detections
        """
        coordinates = None

        # DIA object, DIA source and source have "ra"/"dec" columns
        coordinates_1 = ["ra", "dec"]
        # DIA forced source, object and forced source have "coord_ra"/"coord_dec" columns
        coordinates_2 = ["coord_ra", "coord_dec"]

        if all(col in table.column_names for col in coordinates_1):
            coordinates = coordinates_1
        elif all(col in table.column_names for col in coordinates_2):
            coordinates = coordinates_2
        if coordinates is None:
            raise ValueError("ra/dec columns not found")

        ra_values = table[coordinates[0]]
        dec_values = table[coordinates[1]]
        invalid_ra_mask = pc.is_null(ra_values, nan_is_null=True)
        invalid_dec_mask = pc.is_null(dec_values, nan_is_null=True)
        inverse_mask = pc.or_(invalid_ra_mask, invalid_dec_mask)

        # Filter out all non-primary detections
        if self.filter_non_primary:
            non_primary_mask = pc.invert(table[self.PRIMARY_FLAG_COL])
            inverse_mask = pc.or_(inverse_mask, non_primary_mask)

        filtered_table = table.filter(pc.invert(inverse_mask))
        return filtered_table
