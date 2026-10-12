package br.com.gabriel.pedidoeventos.stock_service.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic estoqueProcessadoTopic(@Value("${app.topico.estoque-processado}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }

    // Tópico que este serviço consome: declarado aqui também para que, na subida, quem chegar
    // primeiro crie o tópico já com 3 partições (senão o broker o cria com 1 ao primeiro subscribe).
    @Bean
    public NewTopic pagamentosProcessadosTopic(@Value("${app.topico.pagamentos-processados}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }

    // Destino das mensagens de pagamentos.processados que esgotaram as tentativas. O order-service
    // usa o mesmo tópico; o grupo de origem fica no cabeçalho kafka_dlt-original-consumer-group.
    @Bean
    public NewTopic pagamentosProcessadosDlqTopic(@Value("${app.topico.pagamentos-processados}") String nome) {
        return TopicBuilder.name(nome + ".dlq").partitions(1).replicas(1).build();
    }
}
