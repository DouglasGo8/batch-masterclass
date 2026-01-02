drop table if exists video_game_sales;

create table video_game_sales
(
    rank         int,
    name         text,
    platform     text,
    year         int,
    genre        text,
    publisher    text,
    na_sales     numeric(5, 2),
    eu_sales     numeric(5, 2),
    jp_sales     numeric(5, 2),
    other_sales  numeric(5, 2),
    global_sales numeric(5, 2)
    --unique (name)
);

select * from video_game_sales;

create table year_platform_report
(
    year     int,
    platform text,
    sales    numeric(5, 2),
    unique (year, platform)
);

insert into year_platform_report
select distinct year, platform, sum(global_sales)
from video_game_sales
group by year, platform;

--
CREATE MATERIALIZED VIEW mv_sales_by_year AS
SELECT year,
       SUM(na_sales + eu_sales + other_sales + global_sales) AS total_sales,
       platform
FROM video_game_sales
GROUP BY year, platform;
CREATE INDEX idx_mv_year ON mv_sales_by_year (year, platform);
--
ANALYZE mv_sales_by_year;

-- select * from video_game_sales
-- truncate table video_game_sales
-- Rank,Name,Platform,Year,Genre,Publisher,NA_Sales,EU_Sales,JP_Sales,Other_Sales,Global_Sales