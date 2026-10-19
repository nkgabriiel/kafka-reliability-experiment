package br.com.gabriel.pedidoeventos.order_service.kafka;

import br.com.gabriel.pedidoeventos.order_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.order_service.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publica pedidos.criados. Com a proteção outbox ligada (v4 em diante), o evento é gravado na outbox
 * dentro da transação do pedido e publicado pelo poller. Desligada (v1 a v3), é enviado ao Kafka
 * dentro da própria transação, antes do commit.
 */
@Component
@RequiredArgsConstructor
public class PedidoEventoProducer {

    private final OutboxWriter outboxWriter;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.topico.pedidos-criados}")
    private String topico;

    @Value("${app.demo.atraso-publicacao-ms:0}")
    private long atrasoPublicacaoMs;

    @Value("${app.protecao.outbox:false}")
    private boolean outboxAtivo;

    /** Chamado dentro da transação que grava o pedido. */
    public void publicarNaTransacao(PedidoCriadoEvent evento) {
        String chave = evento.pedidoId().toString();
        if (outboxAtivo) {
            outboxWriter.registrar(topico, chave, evento.eventId(), evento);
        } else {
            kafkaTemplate.send(topico, chave, evento);
            dormir(atrasoPublicacaoMs);
        }
    }

    /** Alarga a janela entre o envio e o commit, para os experimentos; 0 = comportamento natural. */
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
