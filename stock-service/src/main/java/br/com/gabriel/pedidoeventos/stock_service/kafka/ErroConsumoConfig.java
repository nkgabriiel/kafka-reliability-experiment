package br.com.gabriel.pedidoeventos.stock_service.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Retry bloqueante com backoff exponencial e jitter. Enquanto a mensagem é tentada de novo, a
 * partição fica parada (preserva a ordem). Esgotadas as tentativas, a mensagem vai para o tópico
 * "<original>.dlq" e o consumo segue para a próxima.
 */
@Slf4j
@Configuration
public class ErroConsumoConfig {

    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.retry.max-retentativas:3}") int maxRetentativas,
            @Value("${app.retry.intervalo-inicial-ms:500}") long intervaloInicialMs,
            @Value("${app.retry.multiplicador:2.0}") double multiplicador,
            @Value("${app.retry.intervalo-maximo-ms:10000}") long intervaloMaximoMs,
            @Value("${app.retry.jitter-ms:200}") long jitterMs) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (registro, erro) -> new TopicPartition(registro.topic() + ".dlq", -1));

        ExponentialBackOff backOff = new ExponentialBackOff(intervaloInicialMs, multiplicador);
        backOff.setMaxInterval(intervaloMaximoMs);
        backOff.setMaxAttempts(maxRetentativas);
        backOff.setJitter(jitterMs);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // confirma o offset da mensagem enviada à DLQ, para o consumo seguir para a próxima
        handler.setCommitRecovered(true);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> registro, Exception erro, int tentativa) {
                log.warn("Tentativa {} falhou para {}-{}@{}: {}", tentativa,
                        registro.topic(), registro.partition(), registro.offset(), erro.getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> registro, Exception erro) {
                log.error("Tentativas esgotadas para {}-{}@{}, enviado para {}.dlq",
                        registro.topic(), registro.partition(), registro.offset(), registro.topic());
            }
        });
        return handler;
    }
}
