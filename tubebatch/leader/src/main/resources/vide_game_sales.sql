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

--------------- pg4Admin

select * from video_game_sales;

select * from  year_platform_report;

select year,
       platform,
       sales,
       count(*) over (partition by year) as platforms_per_year
from year_platform_report
where year != 0
order by year;

/**
truncate table video_game_sales;
truncate table year_platform_report;
**/