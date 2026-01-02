package com.batch.masterclass.tubebatch.leader;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


@Slf4j
@SpringBootApplication
public class TubeBatchApp {

  protected static final String EMPTY_CSV_STATUS = "EMPTY";

  public static void main(String[] args) {
    SpringApplication.run(TubeBatchApp.class, args);
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
  Job job(JobRepository jobRepository, Step taskletStep,
          CsvToDbStepConfiguration csvToDbStep,
          YearPlatformReportStepConfiguration yearPlatformReportStep,
          ErrorStepConfiguration errorStep) {
    //
    return new JobBuilder("job", jobRepository)
            .start(taskletStep).on(EMPTY_CSV_STATUS)
              .to(errorStep.errorStep())
            .from(taskletStep).on("*")
              .to(csvToDbStep.csvToStep())
            .next(yearPlatformReportStep.yearPlatformReportStep())
              .build()
            .build();
  }

  @Bean
  Step taskletStep(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager tx) {
    return new StepBuilder("step1", jobRepository)
            .tasklet(tasklet, tx).build();
  }


  // ----------------------------- Configurations ---------------------------------

  @Configuration
  class CsvToDbStepConfiguration {
    final JdbcTemplate jdbc;
    final Resource resource;
    final DataSource dataSource;
    final JobRepository jobRepository;
    final PlatformTransactionManager tx;

    CsvToDbStepConfiguration(@Value("classpath:/data/vgsales.csv") Resource resource,
                             DataSource dataSource, JobRepository jobRepository,
                             PlatformTransactionManager txm, JdbcTemplate template) {
      //
      this.tx = txm;
      this.jdbc = template;
      this.dataSource = dataSource;
      this.resource = resource;
      this.jobRepository = jobRepository;
    }

    @Bean
    @StepScope
    FlatFileItemReader<VideGameSale> reader() {
      //
      return new FlatFileItemReaderBuilder<VideGameSale>().name("gameByYearReader")
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
    JdbcBatchItemWriter<VideGameSale> writer() {
      var sql = """
              INSERT INTO video_game_sales 
              VALUES( :rank, :name, :platform, :year, :genre, :publisher, 
                      :na_sales, :eu_sales, :jp_sales, :other_sales, :global_sales
                    );
              """;

      return new JdbcBatchItemWriterBuilder<VideGameSale>()
              .sql(sql)
              // --
              .itemSqlParameterSourceProvider((VideGameSale item) -> {
                var map = new HashMap<String, Object>();
                //
                map.putAll(Map.of("rank", item.rank(),
                        "name", item.name(),
                        "platform", item.platform(),
                        "year", item.year(),
                        "genre", item.genre()));
                //
                map.putAll(Map.of("publisher", item.publisher(),
                        "na_sales", item.na_sales(),
                        "eu_sales", item.eu_sales(),
                        "jp_sales", item.jp_sales(),
                        "other_sales", item.other_sales(),
                        "global_sales", item.global_sales()));
                //
                return new MapSqlParameterSource(map);
              })
              .dataSource(dataSource)
              .build();
    }

    private int parseYear(String year) {
      return StringUtils.isNumeric(year) ? Integer.parseInt(year) : 0;
    }

    @Bean
    Step csvToStep() {
      //
      return new StepBuilder(this.jobRepository)
              .<VideGameSale, VideGameSale>chunk(100).transactionManager(tx)
              .reader(this.reader())
              .writer(this.writer())
              .listener(new StepExecutionListener() {
                @Override
                public ExitStatus afterStep(StepExecution stepExecution) {
                  var count = Objects.requireNonNull(
                          jdbc.queryForObject("select coalesce(count(*) ,0) from video_game_sales", Integer.class));
                  var status = count == 0 ? new ExitStatus(TubeBatchApp.EMPTY_CSV_STATUS) : ExitStatus.COMPLETED;
                  System.out.println("the status is " + status);
                  return status;
                }
              })
              .build();
    }

  }

  @Configuration
  @AllArgsConstructor
  class ErrorStepConfiguration {
    final JobRepository jobRepository;
    final PlatformTransactionManager tx;

    @Bean
    Step errorStep() {
      return new StepBuilder("errorStep", this.jobRepository)
              .tasklet(((contribution, chunkContext) -> {
                log.info("OOPS!!! Error");
                return RepeatStatus.FINISHED;
              })).build();
    }
  }

  @Configuration
  @AllArgsConstructor
  class YearPlatformReportStepConfiguration {
    final JdbcTemplate jdbc;
    final TransactionTemplate txt;
    final JobRepository jobRepository;
    final PlatformTransactionManager txm;

    @Bean
    Step yearPlatformReportStep() {
      return new StepBuilder("yearPlatformReportStep", jobRepository)//
              .tasklet((contribution, chunkContext) ->//
                      txt.execute(status -> {
                        jdbc.execute(
                                """
                                            insert into year_platform_report (year, platform)
                                            select year, platform from video_game_sales
                                            on conflict on constraint year_platform_report_year_platform_key do nothing;
                                        """);
                        jdbc.execute("""
                                insert into year_platform_report (year, platform, sales)
                                select yp1.year,
                                       yp1.platform, (
                                            select sum(vgs.global_sales) from video_game_sales vgs
                                            where vgs.platform = yp1.platform and vgs.year = yp1.year
                                        )
                                from year_platform_report as yp1
                                on conflict on constraint year_platform_report_year_platform_key
                                 do update set 
                                            year = excluded.year,
                                        platform = excluded.platform,
                                           sales = excluded.sales;
                                """);
                        return RepeatStatus.FINISHED;
                      }), txm)//
              .build();
    }

  }
}
