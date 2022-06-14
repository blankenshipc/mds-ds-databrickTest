import pyspark.sql.functions as F

def get_tags_summary(TAGS,TABLE, START_TIME, END_TIME):

    df = spark.table(TABLE)

    tags_query_lst = [f"Tag LIKE '{tag}' OR " for tag in TAGS]
    tags_query ="".join(tags_query_lst)[:-3]
    print(tags_query)

    grpd_df = (df
          #.where(f"Tag in ({tags_query}) AND Timestamp BETWEEN '{START_TIME}' and '{END_TIME}'")
          .where(f"{tags_query} AND Timestamp BETWEEN '{START_TIME}' and '{END_TIME}'")
          .groupby("Tag")
          .agg(F.count('Numeric_Value').alias('count'),
             F.mean('Numeric_Value').alias('mean'),
             F.stddev('Numeric_Value').alias('std'),
             F.min('Numeric_Value').alias('min'),
                F.max('Numeric_Value').alias('max'),
             F.expr('percentile(Numeric_Value, array(0.25))')[0].alias('%25'),
             F.expr('percentile(Numeric_Value, array(0.5))')[0].alias('%50'),
             F.expr('percentile(Numeric_Value, array(0.75))')[0].alias('%75')))

    return grpd_df

TAGS = ['R.Precision.599.NOVOS.RotationState', 'R.Precision.540.NOVOS%']
TABLE = "default.pdrigs2"
START_TIME = '2021-01-10T00:00:00'
END_TIME = '2021-01-10T01:00:00'