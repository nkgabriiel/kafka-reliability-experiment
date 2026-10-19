package br.com.gabriel.pedidoeventos.payment_service.kafka;

import br.com.gabriel.pedidoeventos.payment_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publica pagamentos.processados. Com a proteção outbox ligada (v4 em diante), o evento é gravado na
 * outbox dentro da transação e publicado pelo poller; desligada (v1 a v3), é enviado ao Kafka pelo
 * listener depois do commit.
 */
@Component
@RequiredArgsConstructor
public class PagamentoEventoProducer {

    private final OutboxWriter outboxWriter;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.topico.pagamentos-processados}")
    private String topico;

    @Value("${app.protecao.outbox:false}")
    private boolean outboxAtivo;

    /** Chamado dentro da transação que grava o pagamento. Sem outbox, não faz nada. */
    public void registrarNaTransacao(PagamentoProcessadoEvent evento) {
        if (outboxAtivo) {
            outboxWriter.registrar(topico, evento.pedidoId().toString(), evento.eventId(), evento);
        }
    }

    /** Chamado pelo listener depois do commit. Com outbox, não faz nada (o poller publica). */
    public void publicarAposCommit(PagamentoProcessadoEvent evento) {
        if (!outboxAtivo) {
            kafkaTemplate.send(topico, evento.pedidoId().toString(), evento);
        }
    }
}
