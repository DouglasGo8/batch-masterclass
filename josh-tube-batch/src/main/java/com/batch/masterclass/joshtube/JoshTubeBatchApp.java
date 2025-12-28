package com.batch.masterclass.joshtube;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;


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
  Tasklet tasklet() {
    return (contribution, context) -> {
      log.info("Tasklet started");
      return RepeatStatus.FINISHED;
    };
  }

  @Bean
  Job job(JobRepository jobRepository, Step step1) {
    return new JobBuilder("job1", jobRepository)
            .start(step1).build();
  }

  @Bean
  Step step1(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager tx) {
    return new StepBuilder("step1", jobRepository)
            .tasklet(tasklet, tx).build();
  }


}
