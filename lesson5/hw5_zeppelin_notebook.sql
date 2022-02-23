spark.sql
create table if not exists videogame
	using csv
	options (
		path "/user/hduser/videogame/vgsales.csv",
		header true,
		interSchema true
	);

SELECT * FROM videogame LIMIT 10;

SELECT * FROM videogame ORDER BY Rank asc LIMIT 1;

# Какая платформа самая популярная в каждом регионе (NA, EU, JP)?

WITH x as (
	SELECT
		Platform,
		sum(NA_Sales) NA_Sales_Sum,
		sum(EU_Sales) EU_Sales_Sum,
		sum(JP_Sales) JP_Sales_Sum
	FROM videogame
	GROUP BY Platform
)
(
	SELECT 'NA' as region, Platform
	FROM x
	ORDER BY NA_Sales_Sum desc
	LIMIT 1
)
union
(
	SELECT 'EU' as region, Platform
	FROM x
	ORDER BY EU_Sales_Sum desc
	LIMIT 1
)
union
(
	SELECT 'JP' as region, Platform
	FROM x
	ORDER BY JP_Sales_Sum desc
	LIMIT 1
)

# Какой жанр популярен больше всего в каждом регионе (NA, EU, JP)?

WITH x as (
	SELECT
		Genre,
		sum(NA_Sales) NA_Sales_Sum,
		sum(EU_Sales) EU_Sales_Sum,
		sum(JP_Sales) JP_Sales_Sum
	FROM videogame
	GROUP BY Genre
)
(
	SELECT 'NA' as region, Genre
	FROM x
	ORDER BY NA_Sales_Sum desc
	LIMIT 1
)
union
(
	SELECT 'EU' as region, Genre
	FROM x
	ORDER BY EU_Sales_Sum desc
	LIMIT 1
)
union
(
	SELECT 'JP' as region, Genre
	FROM x
	ORDER BY JP_Sales_Sum desc
	LIMIT 1
)

WITH x as (
	SELECT
		Genre,
		Year,
		sum(Global_Sales) Global_Sales,
		row_number() over(partition by Year ORDER BY sum(Global_Sales) desc) as row_number
	FROM videogame
	WHERE Year != 'N/A'
	GROUP BY Year, Genre
	ORDER BY Year, Global_Sales desc
)
SELECT Year, Genre
FROM x
WHERE row_number == 1

## GROUP-BY
WITH x as (
	SELECT
		Genre, max(NA_Sales)
	FROM videogame
	GROUP BY Genre
)

