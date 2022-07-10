DECLARE @StartDate DATE  = '2010-01-01'; -- Start-date
DECLARE @EndDate DATE =  CAST(YEAR(GETDATE()) + 2 AS CHAR(4)) + '-01-01'; -- End-Date

DROP TABLE IF EXISTS [master].[TimeTable];

CREATE TABLE [master].[TimeTable] (
	[utc_datetime] [datetime] NOT NULL,
	[utc_int] [bigint] NULL,
	[utc_year] [smallint] NULL,
	[minute] [smallint] NULL,
	[pacific_timezone_offset] [smallint] NULL,
	[pacific_current_offset] [smallint] NULL,
	[pacific_datetime] [datetime] NULL,
	[mountain_timezone_offset] [smallint] NULL,
	[mountain_current_offset] [smallint] NULL,
	[mountain_datetime] [datetime] NULL,
	[central_timezone_offset] [smallint] NULL,
	[central_current_offset] [smallint] NULL,
	[central_datetime] [datetime] NULL,
	[eastern_timezone_offset] [smallint] NULL,
	[eastern_current_offset] [smallint] NULL,
	[eastern_datetime] [datetime] NULL,
	CONSTRAINT [PK_TimeTable] PRIMARY KEY CLUSTERED ([utc_datetime] ASC)
);

DROP TABLE IF EXISTS #DateTimeTable;

SELECT
	  YEAR(CalculatedDate) AS [utc_year]
	, CAST(CONCAT(CONVERT(NVARCHAR, CalculatedDate, 23), ' ', [date_hour] - 1, ':00:00') AS DATETIME) AS [utc_datetime]
INTO #DateTimeTable
FROM (
	SELECT CalculatedDate = CAST(DATEADD(DAY, rn - 1, @StartDate) AS DATE)
	FROM (
		SELECT TOP (DATEDIFF(DAY, @StartDate, @EndDate)) rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
		FROM sys.all_objects AS s1
			CROSS JOIN sys.all_objects AS s2
		ORDER BY s1.[object_id]
	) AS x
) d
	CROSS APPLY (
		SELECT TOP 24 [date_hour] = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
		FROM sys.all_objects AS s1
			CROSS JOIN sys.all_objects AS s2
		ORDER BY s1.[object_id]
	) t

;WITH TimezoneData AS (
	SELECT 
		  [utc_datetime]
		, CASE	
			WHEN YEAR([utc_datetime]) < 1966 THEN NULL
			WHEN [utc_datetime] < [start_datetime] OR [utc_datetime] >= [end_datetime] THEN [timezone_offset] - 1
			ELSE [timezone_offset]
			END AS [current_offset]
		, [timezone]
		, [timezone_offset]
		, [start_datetime]
		, [end_datetime]
	FROM #DateTimeTable d
		INNER JOIN (
			SELECT
				  [calendar_year]
				, [timezone]
				, [timezone_offset]
				, CASE 
					WHEN [calendar_year] < 1966 THEN NULL
					WHEN [calendar_year] >= 2007 THEN [start_date]
					ELSE DATEADD(WEEK, 3, [start_date])
					END AS [start_datetime]
				, CASE 
					WHEN [calendar_year] < 1966 THEN NULL
					WHEN [calendar_year] >= 2007 THEN [end_date]
					ELSE DATEADD(WEEK, -1, [end_date])
					END AS [end_datetime]
			FROM (
				SELECT 
					  [calendar_year]
					, tmz.timezone
					, tmz.timezone_offset
					, CASE DATEPART(dw, [start_week])
						WHEN 1 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 171, [start_week])
						WHEN 2 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 315, [start_week])
						WHEN 3 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 291, [start_week])
						WHEN 4 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 267, [start_week])
						WHEN 5 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 243, [start_week])
						WHEN 6 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 219, [start_week])
						WHEN 7 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 195, [start_week])
						END AS [start_date]
					, CASE DATEPART(dw, [end_week])
						WHEN 1 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 2, [end_week])
						WHEN 2 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 146, [end_week])
						WHEN 3 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 122, [end_week])
						WHEN 4 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 98, [end_week])
						WHEN 5 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 74, [end_week])
						WHEN 6 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 50, [end_week])
						WHEN 7 THEN DATEADD(hour, ABS(tmz.timezone_offset) + 26, [end_week])
						END AS [end_date]
				FROM (
					SELECT
						  [calendar_year]
						, CONCAT(convert(varchar, calendar_year), '/03/01') AS [start_week]
						, CONCAT(convert(varchar, calendar_year), '/11/01') AS [end_week]
					FROM (
						SELECT YEAR(@StartDate) - 1 + rn AS [calendar_year]
						FROM (
							SELECT TOP (YEAR(GETDATE()) - YEAR(@StartDate) + 2) rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
							FROM sys.all_objects AS s1
								CROSS JOIN sys.all_objects AS s2
							ORDER BY s1.[object_id]
						) y
					) y
				) c
					CROSS APPLY (
						SELECT
							CASE	
								WHEN [timezone_offset] = 4 THEN 'Eastern'
								WHEN [timezone_offset] = 5 THEN 'Central'
								WHEN [timezone_offset] = 6 THEN 'Mountain'
								WHEN [timezone_offset] = 7 THEN 'Pacific'
							END AS [timezone]
							, [timezone_offset] * -1 AS [timezone_offset]
						FROM (
							SELECT 
								[time_zone] + 3 AS [timezone_offset]
							FROM (
								SELECT TOP 4 [time_zone] = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
								FROM sys.all_objects AS s1
									CROSS JOIN sys.all_objects AS s2
								ORDER BY s1.[object_id]
							) tmz
						) tmz
					) tmz
			) dst
		) dst ON d.utc_year = dst.calendar_year
	) 
	
INSERT INTO [master].[TimeTable] (
	  [utc_datetime]
	, [utc_int]
	, [utc_year]
	, [minute]
	, [pacific_timezone_offset]
	, [pacific_current_offset]
	, [pacific_datetime]
	, [mountain_timezone_offset]
	, [mountain_current_offset]
	, [mountain_datetime]
	, [central_timezone_offset]
	, [central_current_offset]
	, [central_datetime]
	, [eastern_timezone_offset]
	, [eastern_current_offset]
	, [eastern_datetime]
)
SELECT DISTINCT
	  DATEADD(MINUTE, m.minute, [utc_datetime]) AS [utc_datetime]
	, CONVERT(BIGINT, (DATEDIFF(ss, '01-01-1970 00:00:00', DATEADD(MINUTE, m.minute, [utc_datetime])))) / 10 AS [utc_int]
	, [utc_year]
	, m.minute AS [minute]
	, [pacific_timezone_offset]
	, [pacific_current_offset]
	, DATEADD(MINUTE, m.minute, [pacific_datetime]) AS [pacific_datetime]
	, [mountain_timezone_offset]
	, [mountain_current_offset]
	, DATEADD(MINUTE, m.minute, [mountain_datetime]) AS [mountain_datetime]
	, [central_timezone_offset]
	, [central_current_offset]
	, DATEADD(MINUTE, m.minute, [central_datetime]) AS [central_datetime]
	, [eastern_timezone_offset]
	, [eastern_current_offset]
	, DATEADD(MINUTE, m.minute, [eastern_datetime]) AS [eastern_datetime]
FROM (
	SELECT DISTINCT
		  d.utc_datetime AS [utc_datetime]		
		, d.utc_year AS [utc_year]
		, p.timezone_offset AS [pacific_timezone_offset]
		, p.current_offset AS [pacific_current_offset]
		, DATEADD(hh, p.current_offset, d.utc_datetime) AS [pacific_datetime]
		, m.timezone_offset AS [mountain_timezone_offset]
		, m.current_offset AS [mountain_current_offset]
		, DATEADD(hh, m.current_offset, d.utc_datetime) AS [mountain_datetime]
		, c.timezone_offset AS [central_timezone_offset]
		, c.current_offset AS [central_current_offset]
		, DATEADD(hh, c.current_offset, d.utc_datetime) AS [central_datetime]
		, e.timezone_offset AS [eastern_timezone_offset]
		, e.current_offset AS [eastern_current_offset]
		, DATEADD(hh, e.current_offset, d.utc_datetime) AS [eastern_datetime]
	FROM #DateTimeTable d
		LEFT OUTER JOIN TimezoneData e ON d.utc_datetime = e.utc_datetime AND e.timezone = 'Eastern'
		LEFT OUTER JOIN TimezoneData c ON d.utc_datetime = c.utc_datetime AND c.timezone = 'Central'
		LEFT OUTER JOIN TimezoneData m ON d.utc_datetime = m.utc_datetime AND m.timezone = 'Mountain'
		LEFT OUTER JOIN TimezoneData p ON d.utc_datetime = p.utc_datetime AND p.timezone = 'Pacific'
) tmz
	CROSS APPLY (
		SELECT TOP 60 [minute] = ROW_NUMBER() OVER (ORDER BY s1.[object_id]) - 1
        FROM sys.all_objects AS s1
            CROSS JOIN sys.all_objects AS s2
        ORDER BY s1.[object_id]
	) AS m
ORDER BY [utc_datetime];