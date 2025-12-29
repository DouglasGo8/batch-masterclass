package com.batch.masterclass.joshtube;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ItemStreamWriter;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.util.Map;


@Slf4j
@SpringBootApplication
public class JoshTubeBatchApp {

  public static void main(String[] args) {
    SpringApplication.run(JoshTubeBatchApp.class, args);
  }


  @Bean
  public CommandLineRunner runJob(JobOperator jobOperator, Job job) {
    return args -> {
      try {
        log.info(">>> Launching Batch 6.x Job via JobOperator...");
        // JobOperator uses the Job Name string and a string of parameters
        var param = new JobParametersBuilder().addLong("time", System.currentTimeMillis()).toJobParameters();
        jobOperator.start(job, param);
      } catch (Exception e) {
        log.error("Failed to start job", e);
      }
    };
  }

  @Bean
  @StepScope
  Tasklet tasklet(@Value("#{jobParameters['time']}") long time) {
    return (contribution, context) -> {
      log.info("Tasklet started with params, {}", time);
      return RepeatStatus.FINISHED;
    };
  }

  @Bean
  Job job(JobRepository jobRepository, Step step, Step csvToJob) {
    return new JobBuilder("job1", jobRepository)
            //.incrementer(new RunIdIncrementer())
            .start(step)
            .next(csvToJob)
            .build();
  }

  int parseYear(String year) {
    return StringUtils.isNumeric(year) ? Integer.parseInt(year) : 0;
  }


  @Bean
  FlatFileItemReader<VideGameSale> csvRowToItemReader(@Value("classpath:/data/vgsales.csv") Resource resource) {
    //
    return new FlatFileItemReaderBuilder<VideGameSale>().name("vgsalesItemReader")
            .resource(resource)
            .delimited().delimiter(",")
            .names(new String[]{"rank", "name",
                    "platform", "year", "genre", "publisher",
                    "na_sales", "eu_sales", "jp_sales",
                    "other_sales", "global_sales"})
            //.names("rank,name,platform,year,genre,publisher,na_sales,eu_sales,jp_sales,other_sales,global_sales".split(","))
            // ------------------
            .linesToSkip(1)
            // ------------------------
            .fieldSetMapper(fieldSet -> new VideGameSale(
                    fieldSet.readInt("rank"),
                    fieldSet.readString("name"),
                    fieldSet.readString("platform"),
                    this.parseYear(fieldSet.readString("year")),
                    fieldSet.readString("genre"),
                    fieldSet.readString("publisher"),
                    fieldSet.readFloat("na_sales"),
                    fieldSet.readFloat("eu_sales"),
                    fieldSet.readFloat("jp_sales"),
                    fieldSet.readFloat("other_sales"),
                    fieldSet.readFloat("global_sales")
            ))
            .build();
  }

  @Bean
  JdbcBatchItemWriter<VideGameSale> videoGameSaleItemWriter(DataSource dataSource) {
    var sql = """
            INSERT INTO video_game_sales 
            VALUES( :rank, :name, :platform, :year, :genre, :publisher, 
                    :na_sales, :eu_sales, :jp_sales, :other_sales
                  );
            """;
    return new JdbcBatchItemWriterBuilder<VideGameSale>()
            .sql(sql)
            // --
            .itemSqlParameterSourceProvider((VideGameSale item) -> new MapSqlParameterSource(Map.of(
                    "rank", item.rank(),
                    "name", item.name(),
                    "platform", item.platform(),
                    "year", item.year(),
                    "genre", item.genre(),
                    "publisher", item.publisher(),
                    "na_sales", item.na_sales(),
                    "eu_sales", item.eu_sales(),
                    "jp_sales", item.jp_sales(),
                    "other_sales", item.other_sales())))
            // ---
            /*.itemPreparedStatementSetter((VideGameSale item, PreparedStatement ps) -> {
              ps.setInt(0, item.rank());
              ps.setString(1, item.name());
              ps.setString(2, item.platform());
              ps.setInt(3, item.year());
              ps.setString(4, item.genre());
              ps.setString(5, item.publisher());
              ps.setFloat(6, item.na_sales());
              ps.setFloat(7, item.eu_sales());
              ps.setFloat(8, item.jp_sales());
              ps.setFloat(9, item.other_sales());
              ps.setFloat(10, item.global_sales());
            })*/
            .dataSource(dataSource)
            .build();
  }

  @Bean
  Step csvToJob(JobRepository jobRepository, PlatformTransactionManager tx,
                FlatFileItemReader<VideGameSale> csvRowToItemReader,
                JdbcBatchItemWriter<VideGameSale> videGameSaleJdbcBatchItemWriter) {
    //
    return new StepBuilder(jobRepository)
            .<VideGameSale, VideGameSale>chunk(100).transactionManager(tx)
            .reader(csvRowToItemReader)
            .writer(videGameSaleJdbcBatchItemWriter)
            .build();
  }


  @Bean
  Step step(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager tx) {
    return new StepBuilder("step1", jobRepository)
            .tasklet(tasklet, tx).build();
  }

  /*@Bean
  JdbcTemplate jdbcTemplate(DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }*/


}
