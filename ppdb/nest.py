"""Utilities to nest sources in objects"""


def nest_sources(dia_object, dia_source, dia_forced_source):
    """Create nested increment dataset"""
    dia_object_lc = (
        dia_object.join_nested(
            dia_source,
            left_on="diaObjectId",
            right_on="diaObjectId",
            nested_column_name="diaSource",
            how="left",
        )
        .join_nested(
            dia_forced_source,
            left_on="diaObjectId",
            right_on="diaObjectId",
            nested_column_name="diaForcedSource",
            how="left",
        )
        .map_partitions(
            lambda x: sort_nested_sources(
                x, source_cols=["diaSource", "diaForcedSource"]
            )
        )
    )
    return dia_object_lc


def sort_nested_sources(df, source_cols, mjd_col="midpointMjdTai"):
    """Sort nested sources by mjd"""
    for source_col in source_cols:
        flat_sources = df[source_col].nest.to_flat()
        df = df.drop(columns=[source_col]).join_nested(
            flat_sources.sort_values([flat_sources.index.name, mjd_col]), source_col
        )
    return df
