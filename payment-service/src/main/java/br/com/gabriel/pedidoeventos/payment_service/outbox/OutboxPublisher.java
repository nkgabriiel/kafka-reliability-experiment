package br.com.gabriel.pedidoeventos.payment_service.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Varre a outbox e publica no Kafka os eventos ainda não enviados. A garantia é at-least-once:
 * se o serviço cair depois do envio e antes de marcar como enviado, o evento é publicado de novo
 * no próximo ciclo, e o dedupe por eventId dos consumers descarta a repetição.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "app.protecao.outbox", havingValue = "true")
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final ObjectMapper MAPPER = JacksonUtils.enhancedObjectMapper();
    private static final long TIMEOUT_CONFIRMACAO_SEGUNDOS = 10;

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.demo.atraso-publicacao-ms:0}")
    private long atrasoPublicacaoMs;

    // V6b: com o producer transacional ligado, todo envio ao Kafka precisa estar dentro de uma transação
    @Value("${app.protecao.transacoes:false}")
    private boolean transacoes;

    private void enviar(OutboxEvent outbox, Object evento) throws Exception {
        if (!transacoes) {
            kafkaTemplate.send(outbox.getTopico(), outbox.getChave(), evento)
                    .get(TIMEOUT_CONFIRMACAO_SEGUNDOS, TimeUnit.SECONDS);
            return;
        }
        // esta transação Kafka cobre só o envio do evento; o offset da mensagem de entrada é do container do listener
        kafkaTemplate.executeInTransaction(operacoes -> {
            try {
                operacoes.send(outbox.getTopico(), outbox.getChave(), evento)
                        .get(TIMEOUT_CONFIRMACAO_SEGUNDOS, TimeUnit.SECONDS);
                return null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Scheduled(fixedDelayString = "${app.outbox.intervalo-ms:500}")
    @Transactional
    public void publicarPendentes() {
        List<OutboxEvent> pendentes = repository.findTop50ByEnviadoEmIsNullOrderByCriadoEmAsc();
        for (OutboxEvent outbox : pendentes) {
            // para experimentos: o evento só fica elegível depois da idade configurada (os pendentes vêm do mais antigo)
            if (atrasoPublicacaoMs > 0 && outbox.getCriadoEm().plusMillis(atrasoPublicacaoMs).isAfter(Instant.now())) {
                break;
            }
            try {
                Object evento = MAPPER.readValue(outbox.getPayload(), Class.forName(outbox.getTipo()));
                // espera a confirmação do broker antes de marcar como enviado
                enviar(outbox, evento);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // para o lote aqui para não publicar fora de ordem; o próximo ciclo tenta de novo
                log.warn("Falha ao publicar evento {} da outbox, tentando no próximo ciclo: {}",
                        outbox.getId(), e.toString());
                return;
            }
            outbox.setEnviadoEm(Instant.now());
            log.info("Evento {} publicado em {} a partir da outbox", outbox.getId(), outbox.getTopico());
        }
    }
}
