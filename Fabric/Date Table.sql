ALTER   PROCEDURE [dbo].[Initialize_DateTable]
AS
BEGIN
	SET NOCOUNT ON;

-- Set variables
	DECLARE @StartDate DATE  = '2020-01-01'; -- Start-date
	DECLARE @EndDate DATE =  CAST(YEAR(GETDATE()) + 2 AS CHAR(4)) + '-01-01'; -- End-Date
	DECLARE @days INT = DATEDIFF(day, @StartDate, @EndDate);

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
			[modified_date] [datetime2](0) NULL,
--			CONSTRAINT [PK_dbo_holiday] PRIMARY KEY CLUSTERED ([holiday_key] ASC)
		);
	END

-- Drop table if it already exists
    IF EXISTS (SELECT * FROM sysobjects WHERE name= N'Date' AND XTYPE='U')
		DROP TABLE [dbo].[Date];
-- Create new table

    CREATE TABLE [dbo].[Date](
        [FullDate] [date] NOT NULL,
		[DayOfWeek] [int] NULL ,
		[DayOfWeekName] [varchar](10) NULL,
		[DayOfWeekAbbrev] [varchar](5) NULL,
		[DayOfMonth] [int] NULL,
		[DayOfQuarter] [int] NULL,
		[DayOfYear] [int] NULL,
		[WeekOfYear] [int] NULL,
		[MonthOfYear] [int] NULL,
		[MonthName] [varchar](10) NULL,
		[MonthNameAbbrev] [varchar](5) NULL,
		[QuarterOfYear] [int] NULL,
		[Year] [int] NULL,
		[FiscalDayOfMonth] [int] NULL,
		[FiscalDayOfQuarter] [int] NULL,
		[FiscalDayOfYear] [int] NULL,
		[FiscalWeekOfYear] [int] NULL,
		[FiscalMonthOfYear] [int] NULL,
		[FiscalPeriod] [int] NULL,
		[FiscalQuarterOfYear] [int] NULL,
		[FiscalYear] [int] NULL,
		[IsBusinessDay] [bit] NULL,
		[IsWeekday] [bit] NULL,
		[IsHoliday] [bit] NULL,
		[YYYYMM] [char](6) NULL,
		[YYYYMMDD] [char](8) NULL,
		[MMMYYYY] [char](9) NULL,
		[MMYYYY] [char](7) NULL,
		[YYYYQQ] [char](7) NULL,
		[YYYYFQ] [char](7) NULL
);

-- Build new date table data
	INSERT INTO [dbo].[Date] (
		  [FullDate]
		, [DayOfWeek]
		, [DayOfWeekName]
		, [DayOfWeekAbbrev]
		, [DayOfMonth]
		, [DayOfQuarter]
		, [DayOfYear]
		, [WeekOfYear]
		, [MonthOfYear]
		, [MonthName]
		, [MonthNameAbbrev]
		, [QuarterOfYear]
		, [Year]
		, [FiscalDayOfMonth]
		, [FiscalDayOfQuarter]
		, [FiscalDayOfYear]
		, [FiscalWeekOfYear]
		, [FiscalMonthOfYear]
		, [FiscalQuarterOfYear]
		, [FiscalYear]
		, [IsBusinessDay]
		, [IsWeekday]
		, [IsHoliday]
		, [YYYYMM]
		, [YYYYMMDD]
		, [MMMYYYY]
		, [MMYYYY]
		, [YYYYQQ]
		, [YYYYFQ]
	)
		SELECT
		  a.CurrDate AS [FullDate]
		, DATEPART(DW, a.CurrDate) AS [DayOfWeek]
		, DATENAME(DW, a.CurrDate) AS [DayOfWeekName]
		, CASE WHEN DATEPART(DW, a.CurrDate) IN (3, 5) THEN LEFT(DATENAME(DW, a.CurrDate), 4)
			ELSE LEFT(DATENAME(DW, a.CurrDate), 3)
			END AS [DayOfWeekAbbrev]
		, DATEPART(DD, a.CurrDate) AS [DayOfMonth]
		, DATEDIFF(DD,
			DATEADD(qq, DATEDIFF(qq, 0, a.CurrDate), 0),
			a.CurrDate) + 1
			AS [DayOfQuarter]
		, DATEPART(DY, a.CurrDate) AS [DayOfYear]
		, DATEPART(WW, a.CurrDate) AS [WeekOfYear]
		, DATEPART(MM, a.CurrDate) AS [MonthOfYear]
		, DATENAME(MM, a.CurrDate) AS [MonthName]
		, CASE WHEN DATEPART(MM, a.CurrDate) = 9 THEN LEFT(DATENAME(MM, a.CurrDate), 4)
			ELSE LEFT(DATENAME(MM, a.CurrDate), 3)
			END AS [MonthNameAbbrev]
		, DATEPART(QQ, a.CurrDate) AS [QuarterOfYear]
		, DATEPART(YY, a.CurrDate) AS [Year]
		, DATEPART(DD, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [FiscalDayOfMonth]
		, DATEDIFF(DD,
			DATEADD(qq, DATEDIFF(qq, 0, DATEADD(m, @FiscalDifference, a.CurrDate)), 0),
			DATEADD(m, @FiscalDifference, a.CurrDate)) + 1
			AS [FiscalDayOfQuarter]
		, DATEPART(DY, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [FiscalDayOfYear]
		, DATEPART(WW, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [FiscalWeekOfYear]
		, DATEPART(MM, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [FiscalMonthOfYear]
		, DATEPART(QQ, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [FiscalQuarterOfYear]
		, DATEPART(YY, DATEADD(m, @FiscalDifference, a.CurrDate)) AS [FiscalYear]
		, CASE WHEN DATEPART(DW, a.CurrDate) = 1 OR DATEPART(DW, a.CurrDate) = 7 OR h.[holiday_key] IS NOT NULL THEN 0
			ELSE 1
			END AS [IsBusinessDay]
		, CASE WHEN DATEPART(DW, a.CurrDate) = 1 OR DATEPART(DW, a.CurrDate) = 7 THEN 0
			ELSE 1
			END AS [IsWeekday]
		, CASE WHEN h.[holiday_key] IS NULL THEN 0
			ELSE 1
			END AS [IsHoliday]
		, LEFT(CONVERT(VARCHAR, a.CurrDate, 112), 6) AS [YYYYMM]
		, CONVERT(VARCHAR, a.CurrDate, 112) AS [YYYYMMDD]
		, CASE WHEN DATEPART(MM, a.CurrDate) = 9 THEN LEFT(DATENAME(MM, a.CurrDate), 4) ELSE LEFT(DATENAME(MM, a.CurrDate), 3) END + ' ' + CAST(DATEPART(YY, a.CurrDate) AS char(4)) AS [MMMYYYY]
		, CASE WHEN MONTH(a.CurrDate) < 10 THEN '0' ELSE '' END + CAST(DATEPART(MM, a.CurrDate) AS varchar(4)) + '-' + CAST(DATEPART(YY, a.CurrDate) AS char(4)) AS [MMYYYY]
		, CAST(DATEPART(YY, a.CurrDate) AS char(4)) + '/Q' + CAST(DATEPART(QQ, a.CurrDate) AS char(1)) AS [YYYYQQ]
		, CAST(DATEPART(YY, DATEADD(m, @FiscalDifference, a.CurrDate)) AS char(4)) + '/Q' + CAST(DATEPART(QQ, DATEADD(m, @FiscalDifference, a.CurrDate)) AS char(1)) AS [YYYYFQ]
    FROM (
        SELECT DATEADD(dd, value - 1, @StartDate) AS CurrDate FROM GENERATE_SERIES(1, @days)
	) a
		LEFT OUTER JOIN [dbo].[Holiday] h ON a.CurrDate = h.observed_date;
END

GO

