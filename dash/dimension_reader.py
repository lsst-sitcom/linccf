import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hats_import.catalog.file_readers import ParquetReader
from lsst.resources import ResourcePath


class DimensionParquetReader(ParquetReader):

    def __init__(self, chunksize=500_000, column_names=None, **kwargs):
        self.chunksize = chunksize
        self.column_names = column_names
        self.kwargs = kwargs

    def read(self, input_file, read_columns=None):
        self.regular_file_exists(input_file, **self.kwargs)

        columns = read_columns or self.column_names

        batch_files = pd.read_csv(input_file)
        added_columns = set(batch_files.columns) - set(["path"])

        batch_size = 0
        batch_tables = []

        for _, row in batch_files.iterrows():
            with ResourcePath(row["path"]).open("rb") as f:
                parquet_file = pq.ParquetFile(f, **self.kwargs)
                # ### Do not attempt to process empty files
                if parquet_file.metadata.num_rows == 0:
                    continue
                # ### end of patch
                for smaller_table in parquet_file.iter_batches(
                    batch_size=self.chunksize, columns=columns
                ):
                    table = pa.Table.from_batches([smaller_table])
                    table = table.replace_schema_metadata()

                    if read_columns is None:
                        ## splitting stage - add in dimension columns
                        for column in added_columns:
                            if column not in table.column_names:
                                table = table.append_column(
                                    column,
                                    [np.full(len(table), fill_value=row[column])],
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
