package br.com.gabriel.pedidoeventos.stock_service.kafka;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.JpaTransactionManager;

/**
 * Com o producer transacional, o Spring Boot cria também um KafkaTransactionManager. Para o
 * {@code @Transactional} do banco continuar usando o JPA (e o container Kafka usar o dele), o
 * gerenciador do JPA é declarado explicitamente como o principal.
 */
@Configuration
@ConditionalOnProperty(name = "app.protecao.transacoes", havingValue = "true")
public class TransacaoKafkaConfig {

    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
