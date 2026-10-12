package br.com.gabriel.pedidoeventos.order_service.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class OutboxWriter {

    // mesmo mapper que o JsonSerializer do Kafka usa, para o payload sair idêntico ao de hoje
    private static final ObjectMapper MAPPER = JacksonUtils.enhancedObjectMapper();

    private final OutboxEventRepository repository;

    /**
     * Grava o evento na outbox. Precisa rodar na mesma transação que altera o estado de negócio,
     * por isso exige uma transação já aberta: ou os dois commitam, ou nenhum.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(String topico, String chave, UUID eventId, Object evento) {
        OutboxEvent outbox = new OutboxEvent();
        outbox.setId(eventId);
        outbox.setTopico(topico);
        outbox.setChave(chave);
        outbox.setTipo(evento.getClass().getName());
        outbox.setPayload(serializar(evento));
        outbox.setCriadoEm(Instant.now());
        repository.save(outbox);
    }

    private String serializar(Object evento) {
        try {
            return MAPPER.writeValueAsString(evento);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar evento para a outbox", e);
        }
    }
}
