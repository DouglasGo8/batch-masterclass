drop table if exists video_game_sales;

create table video_game_sales
(
    rank 			int,
    name 			text,
    platform 		text,
    year 			int,
    genre 			text,
    publisher 		text,
    na_sales 		numeric(5,2),
    eu_sales 		numeric(5,2),
    jp_sales 		numeric(5,2),
    other_sales 	numeric(5,2),
    global_sales 	numeric(5,2)
    --unique (name)
);

-- select * from video_game_sales
-- truncate table video_game_sales
-- Rank,Name,Platform,Year,Genre,Publisher,NA_Sales,EU_Sales,JP_Sales,Other_Sales,Global_Sales