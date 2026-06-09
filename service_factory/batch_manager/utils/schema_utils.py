from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import lit
import pandas as pd

def align_schemas(df, all_columns):
    """
    Aligns a DataFrame (Spark or pandas) to the complete column set.
    
    Parameters:
        df: Spark or pandas DataFrame
        all_columns: list or set of all required columns
    
    Returns:
        Aligned DataFrame with all_columns in the specified order.
    """
    all_columns = list(all_columns)  # ensure orderable
    
    if isinstance(df, SparkDataFrame):
        df_cols = set(df.columns)
        missing = set(all_columns) - df_cols
        for col in missing:
            df = df.withColumn(col, lit(None))
        # Reorder columns
        return df.select(*all_columns)

    elif isinstance(df, pd.DataFrame):
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        return df[all_columns]

    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df)}")
