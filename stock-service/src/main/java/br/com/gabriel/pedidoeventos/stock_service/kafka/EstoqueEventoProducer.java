package br.com.gabriel.pedidoeventos.stock_service.kafka;

import br.com.gabriel.pedidoeventos.stock_service.evento.EstoqueProcessadoEvent;
import br.com.gabriel.pedidoeventos.stock_service.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publica estoque.processado. Com a proteção outbox ligada (v4 em diante), o evento é gravado na
 * outbox dentro da transação e publicado pelo poller; desligada (v1 a v3), é enviado ao Kafka pelo
 * listener depois do commit.
 */
@Component
@RequiredArgsConstructor
public class EstoqueEventoProducer {

    private final OutboxWriter outboxWriter;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.topico.estoque-processado}")
    private String topico;

    @Value("${app.demo.atraso-publicacao-ms:0}")
    private long atrasoPublicacaoMs;

    @Value("${app.protecao.outbox:false}")
    private boolean outboxAtivo;

    /** Chamado dentro da transação que reserva o estoque. Sem outbox, não faz nada. */
    public void registrarNaTransacao(EstoqueProcessadoEvent evento) {
        if (outboxAtivo) {
            outboxWriter.registrar(topico, evento.pedidoId().toString(), evento.eventId(), evento);
        }
    }

    /** Chamado pelo listener depois do commit. Com outbox, não faz nada (o poller publica). */
    public void publicarAposCommit(EstoqueProcessadoEvent evento) {
        if (!outboxAtivo) {
            dormir(atrasoPublicacaoMs);
            kafkaTemplate.send(topico, evento.pedidoId().toString(), evento);
        }
    }

    /** Alarga a janela entre o commit e a publicação, para os experimentos; 0 = comportamento natural. */
    private static void dormir(long ms) {
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
