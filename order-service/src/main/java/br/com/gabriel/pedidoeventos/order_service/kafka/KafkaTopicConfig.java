package br.com.gabriel.pedidoeventos.order_service.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic pedidosCriadosTopic(@Value("${app.topico.pedidos-criados}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }

    // Tópicos que este serviço consome: declarados aqui também para que, na subida, quem chegar
    // primeiro crie o tópico já com 3 partições (senão o broker o cria com 1 ao primeiro subscribe).
    @Bean
    public NewTopic pagamentosProcessadosTopic(@Value("${app.topico.pagamentos-processados}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic estoqueProcessadoTopic(@Value("${app.topico.estoque-processado}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }

    // Destinos das mensagens que esgotaram as tentativas. pagamentos.processados.dlq é compartilhado
    // com o stock-service; o grupo de origem fica no cabeçalho kafka_dlt-original-consumer-group.
    @Bean
    @ConditionalOnProperty(name = "app.protecao.retry-dlq", havingValue = "true")
    public NewTopic pagamentosProcessadosDlqTopic(@Value("${app.topico.pagamentos-processados}") String nome) {
        return TopicBuilder.name(nome + ".dlq").partitions(1).replicas(1).build();
    }

    @Bean
    @ConditionalOnProperty(name = "app.protecao.retry-dlq", havingValue = "true")
    public NewTopic estoqueProcessadoDlqTopic(@Value("${app.topico.estoque-processado}") String nome) {
        return TopicBuilder.name(nome + ".dlq").partitions(1).replicas(1).build();
    }
}
