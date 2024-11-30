CREATE PROCEDURE [dbo].[Initialize_DateTable]
AS
BEGIN
	SET NOCOUNT ON;
	
	-- Set variables
	DECLARE @StartDate DATE  = '2020-01-01'; -- Start-date
	DECLARE @EndDate DATE =  CAST(YEAR(GETDATE()) + 2 AS CHAR(4)) + '-01-01'; -- End-Date
	DECLARE @days INT = DATEDIFF(day, @StartDate, @EndDate);
	DECLARE @offset INT = 7 - DATEPART(DW, @StartDate);
	
	DECLARE @DefaultDate DATE = '1900-01-01';
	DECLARE @CurrentDate DATE;

    DECLARE @FiscalStartDate DATE = '2019-09-01';
	DECLARE @FiscalDifference INT = DATEDIFF(m, @FiscalStartDate, @StartDate);

    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name= N'Holiday' AND XTYPE='U')
	BEGIN
		CREATE TABLE [dbo].[Holiday](
			[holiday_key] [varchar](18) NOT NULL,
			[actual_date] [datetime2](0) NOT NULL,
			[observed_date] [datetime2](0) NOT NULL,
			[description] [varchar](32) NULL,
			[created_date] [datetime2](0) NOT NULL,
			[modified_date] [datetime2](0) NULL
		);
	END

	-- Drop table if it already exists
    IF EXISTS (SELECT * FROM sysobjects WHERE name= N'Date' AND XTYPE='U')
		DROP TABLE [dbo].[Date];

	-- Create new table
    CREATE TABLE [dbo].[Date](
        [full_date] [date] NOT NULL,
		[sequential_day] [bigint] NOT NULL,
		[sequential_week] [bigint] NOT NULL,
		[sequential_month] [bigint] NOT NULL,
		[day_of_week] [int] NULL ,
		[day_of_week_name] [varchar](10) NULL,
		[day_of_week_abbrev] [varchar](5) NULL,
		[day_of_month] [int] NULL,
		[day_of_quarter] [int] NULL,
		[day_of_year] [int] NULL,
		[week_of_year] [int] NULL,
		[month_of_year] [int] NULL,
		[month_name] [varchar](10) NULL,
		[month_name_abbrev] [varchar](5) NULL,
		[quarter_of_year] [int] NULL,
		[year] [int] NULL,
		[fiscal_day_of_month] [int] NULL,
		[fiscal_day_of_quarter] [int] NULL,
		[fiscal_day_of_year] [int] NULL,
		[fiscal_week_of_year] [int] NULL,
		[fiscal_month_of_year] [int] NULL,		
		[fiscal_quarter_of_year] [int] NULL,
		[fiscal_year] [int] NULL,
		[is_business_day] [bit] NULL,
		[is_weekday] [bit] NULL,
		[is_holiday] [bit] NULL,
		[yyyymm] [char](6) NULL,
		[yyyymmdd] [char](8) NULL,
		[mmmyyyy] [char](9) NULL,
		[mmyyyy] [char](7) NULL,
		[yyyyqq] [char](7) NULL,
		[yyyyfq] [char](7) NULL
	);

	-- Build new date table data
	INSERT INTO [dbo].[Date] (
		  [full_date]
		, [sequential_day]
		, [sequential_week]
		, [sequential_month]
		, [day_of_week]
		, [day_of_week_name]
		, [day_of_week_abbrev]
		, [day_of_month]
		, [day_of_quarter]
		, [day_of_year]
		, [week_of_year]
		, [month_of_year]
		, [month_name]
		, [month_name_abbrev]
		, [quarter_of_year]
		, [year]
		, [fiscal_day_of_month]
		, [fiscal_day_of_quarter]
		, [fiscal_day_of_year]
		, [fiscal_week_of_year]
		, [fiscal_month_of_year]		
		, [fiscal_quarter_of_year]		
		, [fiscal_year]
		, [is_business_day]
		, [is_weekday]
		, [is_holiday]
		, [yyyymm]
		, [yyyymmdd]
		, [mmmyyyy]
		, [mmyyyy]
		, [yyyyqq]
		, [yyyyfq]
	)
		SELECT
		  a.CurrDate AS [full_date]
		, [sequential_day]
		, (a.sequential_day + @offset - 1 - ((a.sequential_day + @offset - 1) % 7)) / 7 AS [sequential_week]
		, DENSE_RANK() OVER(PARTITION BY 1 ORDER BY FORMAT(a.CurrDate, 'yyyyMM')) AS [sequential_month]
		, DATEPART(DW, a.CurrDate) AS [day_of_week]
		, DATENAME(DW, a.CurrDate) AS [day_of_week_name]
		, CASE WHEN DATEPART(DW, a.CurrDate) IN (3, 5) THEN LEFT(DATENAME(DW, a.CurrDate), 4)
			ELSE LEFT(DATENAME(DW, a.CurrDate), 3)
			END AS [day_of_week_abbrev]
		, DATEPART(DD, a.CurrDate) AS [day_of_month]
		, DATEDIFF(DD,
			DATEADD(qq, DATEDIFF(qq, 0, a.CurrDate), 0),
			a.CurrDate) + 1
			AS [day_of_quarter]
		, DATEPART(DY, a.CurrDate) AS [day_of_year]
		, DATEPART(WW, a.CurrDate) AS [week_of_year]
		, DATEPART(MM, a.CurrDate) AS [month_of_year]
		, DATENAME(MM, a.CurrDate) AS [month_name]
		, CASE WHEN DATEPART(MM, a.CurrDate) = 9 THEN LEFT(DATENAME(MM, a.CurrDate), 4)
			ELSE LEFT(DATENAME(MM, a.CurrDate), 3)
			END AS [month_name_abbrev]
		, DATEPART(QQ, a.CurrDate) AS [quarter_of_year]
		, DATEPART(YY, a.CurrDate) AS [year]
		, DATEPART(DD, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [fiscal_day_of_month]
		, DATEDIFF(DD,
			DATEADD(qq, DATEDIFF(qq, 0, DATEADD(m, @FiscalDifference, a.CurrDate)), 0),
			DATEADD(m, @FiscalDifference, a.CurrDate)) + 1
			AS [fiscal_day_of_quarter]
		, DATEPART(DY, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [fiscal_day_of_year]
		, DATEPART(WW, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [fiscal_week_of_year]
		, DATEPART(MM, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [fiscal_month_of_year]
		, DATEPART(QQ, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [fiscal_quarter_of_year]
		, DATEPART(YY, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [fiscal_year]
		, CASE WHEN DATEPART(DW, a.CurrDate) = 1 OR DATEPART(DW, a.CurrDate) = 7 OR h.[holiday_key] IS NOT NULL THEN 0
			ELSE 1
			END AS [is_business_day]
		, CASE WHEN DATEPART(DW, a.CurrDate) = 1 OR DATEPART(DW, a.CurrDate) = 7 THEN 0
			ELSE 1
			END AS [is_weekday]
		, CASE WHEN h.[holiday_key] IS NULL THEN 0
			ELSE 1
			END AS [is_holiday]
		, LEFT(CONVERT(VARCHAR, a.CurrDate, 112), 6) AS [yyyymm]
		, CONVERT(VARCHAR, a.CurrDate, 112) AS [yyyymmdd]
		, CASE WHEN DATEPART(MM, a.CurrDate) = 9 THEN LEFT(DATENAME(MM, a.CurrDate), 4) ELSE LEFT(DATENAME(MM, a.CurrDate), 3) END + ' ' + CAST(DATEPART(YY, a.CurrDate) AS char(4)) AS [mmmyyyy]
		, CASE WHEN MONTH(a.CurrDate) < 10 THEN '0' ELSE '' END + CAST(DATEPART(MM, a.CurrDate) AS varchar(4)) + '-' + CAST(DATEPART(YY, a.CurrDate) AS char(4)) AS [mmyyyy]
		, CAST(DATEPART(YY, a.CurrDate) AS char(4)) + '/Q' + CAST(DATEPART(QQ, a.CurrDate) AS char(1)) AS [yyyyqq]
		, CAST(DATEPART(YY, DATEADD(m, @FiscalDifference, a.CurrDate)) AS char(4)) + '/Q' + CAST(DATEPART(QQ, DATEADD(m, @FiscalDifference, a.CurrDate)) AS char(1)) AS [yyyyfq]
    FROM (
        SELECT 
			  DATEADD(dd, value - 1, @StartDate) AS CurrDate 
			, value AS [sequential_day]
		FROM GENERATE_SERIES(1, @days)
	) a
		LEFT OUTER JOIN [dbo].[Holiday] h ON a.CurrDate = h.observed_date;
END

GO
