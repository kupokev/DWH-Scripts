# =============================================================================
# CELL 1 — Imports & Configuration
# =============================================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# ---------- Date range ----------
start_date    = date(2020, 1, 1)
end_date      = date(date.today().year + 2, 1, 1)
days          = (end_date - start_date).days       # total rows to generate

# ---------- Fiscal offset ----------
# Fiscal year starts September 1, 2019. Shift every calendar date forward by
# fiscal_difference months to get the equivalent fiscal calendar position.
fiscal_start  = date(2019, 9, 1)
fiscal_diff   = (start_date.year - fiscal_start.year) * 12 + \
                (start_date.month - fiscal_start.month)          # = 4

# ---------- Sequential-week alignment offset ----------
# SQL Server DATEPART(DW): Sunday = 1, Saturday = 7 — same as Spark dayofweek().
# isoweekday(): Mon=1 … Sun=7  →  SQL DW = isoweekday % 7 + 1
sql_dw_of_start = start_date.isoweekday() % 7 + 1               # 2020-01-01 = Wed = 4
week_offset     = 7 - sql_dw_of_start                           # = 3

print(f"Generating {days} rows  |  fiscal_diff={fiscal_diff}  |  week_offset={week_offset}")


# =============================================================================
# CELL 2 — Generate Base Date Spine
# =============================================================================
df = (
    spark.range(1, days + 1)
         .withColumnRenamed("id", "sequential_day")
         .withColumn(
             "full_date",
             F.date_add(
                 F.lit(start_date.isoformat()).cast("date"),
                 (F.col("sequential_day") - 1).cast("int")
             )
         )
)


# =============================================================================
# CELL 3 — Calendar Columns
# =============================================================================

# --- Sequential week (0-based; aligns week boundaries to start_date's week) ---
df = df.withColumn(
    "sequential_week",
    F.floor((F.col("sequential_day") + week_offset - 1) / 7).cast("long")
)

# --- Sequential month (1-based dense rank by calendar month) ---
month_window = Window.orderBy(F.date_format("full_date", "yyyyMM"))
df = df.withColumn(
    "sequential_month",
    F.dense_rank().over(month_window).cast("long")
)

# --- Day of week (Sun=1 … Sat=7, matching SQL Server default DATEFIRST) ---
df = df.withColumn("day_of_week", F.dayofweek("full_date"))

df = df.withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))

# Tuesday (DW 3) and Thursday (DW 5) get 4-char abbreviations; all others 3
df = df.withColumn(
    "day_of_week_abbrev",
    F.when(F.col("day_of_week").isin(3, 5), F.substring("day_of_week_name", 1, 4))
     .otherwise(F.substring("day_of_week_name", 1, 3))
)

df = df.withColumn("day_of_month",   F.dayofmonth("full_date"))
df = df.withColumn("day_of_year",    F.dayofyear("full_date"))
df = df.withColumn("week_of_year",   F.weekofyear("full_date"))  # NOTE: Spark uses ISO-8601 weeks;
                                                                  # SQL Server DATEPART(WW) uses locale-
                                                                  # dependent weeks. Edge-case rows near
                                                                  # Jan 1 may differ by 1 week.
df = df.withColumn("month_of_year",  F.month("full_date"))
df = df.withColumn("quarter_of_year",F.quarter("full_date"))
df = df.withColumn("year",           F.year("full_date"))

df = df.withColumn("month_name", F.date_format("full_date", "MMMM"))
# September gets 4-char abbreviation ("Sept"); all other months get 3
df = df.withColumn(
    "month_name_abbrev",
    F.when(F.month("full_date") == 9, F.substring("month_name", 1, 4))
     .otherwise(F.substring("month_name", 1, 3))
)

# Day of quarter: days elapsed since the first day of the quarter + 1
df = df.withColumn("quarter_start",  F.trunc("full_date", "quarter"))
df = df.withColumn("day_of_quarter", F.datediff("full_date", "quarter_start") + 1)


# =============================================================================
# CELL 4 — Fiscal Columns
# =============================================================================
# Shift every date forward by fiscal_diff months to rebase onto the fiscal
# calendar (fiscal year begins September 1).
df = df.withColumn("fiscal_date", F.add_months("full_date", fiscal_diff))

df = df.withColumn("fiscal_day_of_month",   F.dayofmonth("fiscal_date"))
df = df.withColumn("fiscal_day_of_year",    F.dayofyear("fiscal_date"))
df = df.withColumn("fiscal_week_of_year",   F.weekofyear("fiscal_date"))
df = df.withColumn("fiscal_month_of_year",  F.month("fiscal_date"))
df = df.withColumn("fiscal_quarter_of_year",F.quarter("fiscal_date"))
df = df.withColumn("fiscal_year",           F.year("fiscal_date"))

df = df.withColumn("fiscal_quarter_start",  F.trunc("fiscal_date", "quarter"))
df = df.withColumn(
    "fiscal_day_of_quarter",
    F.datediff("fiscal_date", "fiscal_quarter_start") + 1
)


# =============================================================================
# CELL 5 — Join Holiday Table & Derive Flags
# =============================================================================
holiday_df = (
    spark.table("Holiday")
         .select(F.to_date("observed_date").alias("observed_date"))
         .distinct()
         .withColumn("_is_holiday", F.lit(True))
)

df = df.join(holiday_df, on="observed_date", how="left") \
       .withColumn("_is_holiday", F.coalesce(F.col("_is_holiday"), F.lit(False)))

df = df.withColumn(
    "is_weekday",
    F.when(F.col("day_of_week").isin(1, 7), F.lit(False)).otherwise(F.lit(True))
)
df = df.withColumn("is_holiday",     F.col("_is_holiday"))
df = df.withColumn(
    "is_business_day",
    (F.col("is_weekday") & ~F.col("is_holiday"))
)


# =============================================================================
# CELL 6 — Formatted String Columns
# =============================================================================
df = df.withColumn("yyyymm",  F.date_format("full_date", "yyyyMM"))
df = df.withColumn("yyyymmdd", F.date_format("full_date", "yyyyMMdd"))

df = df.withColumn(
    "mmmyyyy",
    F.concat(F.col("month_name_abbrev"), F.lit(" "), F.year("full_date").cast("string"))
)
df = df.withColumn(
    "mmyyyy",
    F.concat(
        F.when(F.month("full_date") < 10, F.lit("0")).otherwise(F.lit("")),
        F.month("full_date").cast("string"),
        F.lit("-"),
        F.year("full_date").cast("string")
    )
)
df = df.withColumn(
    "yyyyqq",
    F.concat(
        F.year("full_date").cast("string"),
        F.lit("/Q"),
        F.quarter("full_date").cast("string")
    )
)
df = df.withColumn(
    "yyyyfq",
    F.concat(
        F.year("fiscal_date").cast("string"),
        F.lit("/Q"),
        F.quarter("fiscal_date").cast("string")
    )
)


# =============================================================================
# CELL 7 — Select Final Schema & Write Delta Table
# =============================================================================
final_df = df.select(
    F.col("full_date"),
    F.col("sequential_day"),
    F.col("sequential_week"),
    F.col("sequential_month"),
    F.col("day_of_week"),
    F.col("day_of_week_name"),
    F.col("day_of_week_abbrev"),
    F.col("day_of_month"),
    F.col("day_of_quarter"),
    F.col("day_of_year"),
    F.col("week_of_year"),
    F.col("month_of_year"),
    F.col("month_name"),
    F.col("month_name_abbrev"),
    F.col("quarter_of_year"),
    F.col("year"),
    F.col("fiscal_day_of_month"),
    F.col("fiscal_day_of_quarter"),
    F.col("fiscal_day_of_year"),
    F.col("fiscal_week_of_year"),
    F.col("fiscal_month_of_year"),
    F.col("fiscal_quarter_of_year"),
    F.col("fiscal_year"),
    F.col("is_business_day"),
    F.col("is_weekday"),
    F.col("is_holiday"),
    F.col("yyyymm"),
    F.col("yyyymmdd"),
    F.col("mmmyyyy"),
    F.col("mmyyyy"),
    F.col("yyyyqq"),
    F.col("yyyyfq"),
)

(
    final_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("Date")
)

print("Date table created successfully.")
print(f"Row count: {spark.table('Date').count():,}")
