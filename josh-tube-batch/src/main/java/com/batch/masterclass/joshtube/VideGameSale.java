package com.batch.masterclass.joshtube;

// Rank,Name,Platform,Year,Genre,Publisher,NA_Sales,EU_Sales,JP_Sales,Other_Sales,Global_Sales
public record VideGameSale(int rank,
                           String name,
                           String platform,
                           int year,
                           String genre,
                           String publisher,
                           float na_sales,
                           float eu_sales,
                           float jp_sales,
                           float other_sales,
                           float global_sales) {
}
