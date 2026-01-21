"""Utilities to nest sources in objects"""


def nest_sources(dia_object, dia_source=None, dia_forced_source=None):
    """Create nested increment dataset"""
    nested_cat = dia_object
    source_cols = []

    if dia_source is not None:
        nested_cat = nested_cat.join_nested(
            dia_source,
            left_on="diaObjectId",
            right_on="diaObjectId",
            nested_column_name="diaSource",
            how="left",
        )
        source_cols.append("diaSource")
    
    if dia_forced_source is not None:
        nested_cat = nested_cat.join_nested(
            dia_forced_source,
            left_on="diaObjectId",
            right_on="diaObjectId",
            nested_column_name="diaForcedSource",
            how="left",
        )
        source_cols.append("diaForcedSource")

    return nested_cat.map_partitions(sort_nested_sources, source_cols=source_cols)


def sort_nested_sources(df, *, source_cols, mjd_col="midpointMjdTai"):
    """Sort nested sources by mjd"""
    for source_col in source_cols:
        flat_sources = df[source_col].nest.to_flat()
        df = df.drop(columns=[source_col]).join_nested(
            flat_sources.sort_values([flat_sources.index.name, mjd_col]), source_col
        )
    return df
