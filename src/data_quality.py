from pyspark.sql import DataFrame

def compute_null_percent(df: DataFrame, column: str) -> float:
    total_records = df.count()
    null_records = df.filter(df[column].isNull()).count()
    null_percent = (null_records / total_records) * 100
    return null_percent

def compute_quality_metrics(df: DataFrame):
    metrics = {}
    for col in df.columns:
        metrics[col] = compute_null_percent(df, col)
    return metrics
