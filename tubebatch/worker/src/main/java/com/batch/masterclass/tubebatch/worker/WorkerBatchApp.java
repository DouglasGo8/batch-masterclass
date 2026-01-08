package com.batch.masterclass.tubebatch.worker;


import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkRequestHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;

import java.util.Collection;

@Slf4j
@SpringBootApplication
public class WorkerBatchApp {

  public static void main(String[] args) {
    SpringApplication.run(WorkerBatchApp.class, args);
  }


// ----------------------------- Records -------------------------------------

  record YearReport(int year, Collection<YearPlatformSales> breakout) {
  }

  record YearPlatformSales(int year, String platform, float sales) {
  }


  // ----------------------------- Configurations -------------------------------------

  @Configuration
  class WorkerConfiguration {

    @PostConstruct
    public void init() {
      System.setProperty("spring.amqp.deserialization.trust.all", "true");
    }

    @Bean
    org.springframework.amqp.core.Queue requestQueue() {
      return new org.springframework.amqp.core.Queue("requests", false);
    }

    @Bean
    org.springframework.amqp.core.Queue repliesQueue() {
      return new Queue("replies", false);
    }

    @Bean
    TopicExchange exchange() {
      return new TopicExchange("remote-chunking-exchange");
    }

    @Bean
    Binding repliesBinding(TopicExchange exchange) {
      return BindingBuilder.bind(repliesQueue()).to(exchange).with("replies");
    }

    @Bean
    Binding requestBinding(TopicExchange exchange) {
      return BindingBuilder.bind(requestQueue()).to(exchange).with("requests");
    }

    @Bean
    DirectChannel requests() {
      return MessageChannels.direct().getObject();
    }

    @Bean
    DirectChannel replies() {
      return MessageChannels.direct().getObject();
    }

    // For production
    /*
    @Bean
public SimpleMessageConverter converter() {
  var converter = new SimpleMessageConverter();
  converter.setAllowedListPatterns(List.of(
    "org.springframework.batch.integration.chunk.*",
    "org.springframework.batch.core.*",
    "com.batch.masterclass.tubebatch.*",
    "java.util.*",
    "java.lang.*"
  ));
  return converter;
}
     */

    @Bean
    IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {
      return IntegrationFlow
              .from(Amqp.inboundAdapter(connectionFactory, "requests"))
              //.messageConverter(converter()))
              .channel(requests())
              .get();
    }

    @Bean
    IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
      return IntegrationFlow
              .from(replies())
              .handle(Amqp.outboundAdapter(amqpTemplate)
                      .exchangeName("remote-chunking-exchange")
                      .routingKey("replies"))
              .get();
    }

    @Bean
    @ServiceActivator(inputChannel = "requests", outputChannel = "replies", sendTimeout = "10000")
    ChunkProcessorChunkRequestHandler<String> requestHandler() {
      var handler = new ChunkProcessorChunkRequestHandler<String>();

      handler.setChunkProcessor((chunk, contribution) -> {
        log.info("=== Worker processing {} items ===", chunk.getItems().size());

        for (String yearReportJson : chunk.getItems()) {
          try {
            log.info(">> processing YearReport JSON: {}", yearReportJson);
            Thread.sleep(5);

            var mapper = new ObjectMapper();
            YearReport yearReport = mapper.readValue(yearReportJson, YearReport.class);
            log.info("{}", yearReport);

          } catch (Exception e) {
            log.error("Error processing item", e);
            throw new RuntimeException(e);
          }
        }

        log.info("=== Chunk processed successfully ===");
      });

      return handler;
    }
  }

}
