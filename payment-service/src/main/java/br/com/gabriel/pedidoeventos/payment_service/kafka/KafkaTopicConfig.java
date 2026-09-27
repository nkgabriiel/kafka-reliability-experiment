package br.com.gabriel.pedidoeventos.payment_service.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic pagamentosProcessadosTopic(@Value("${app.topico.pagamentos-processados}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }
}