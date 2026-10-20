package br.com.gabriel.pedidoeventos.payment_service.kafka;

import br.com.gabriel.pedidoeventos.payment_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.PagamentoService;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.ProcessamentoPagamento;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PedidoCriadoListener {

    @Value("${app.demo.atraso-ack-ms:0}")
    private long atrasoAckMs;

    private final PagamentoService pagamentoService;
    private final PagamentoEventoProducer pagamentoEventoProducer;

    @KafkaListener(topics = "${app.topico.pedidos-criados}", groupId = "${spring.kafka.consumer.group-id}")
    public void ouvir(PedidoCriadoEvent evento,
                      @Header(name = KafkaHeaders.ACKNOWLEDGMENT, required = false) Acknowledgment ack) {
        MDC.put("pedidoId", evento.pedidoId().toString());
        try {
            log.info("Processando pagamento, valor={}", evento.valorTotal());

            Optional<ProcessamentoPagamento> processado = pagamentoService.processar(evento);
            if (processado.isEmpty()) {
                log.warn("Evento {} já processado, descartando sem reprocessar", evento.eventId());
                confirmar(ack);
                return;
            }

            log.info("Pagamento processado: status={}", processado.get().pagamento().getStatus());

            // sem outbox (v1 a v3) o evento é enviado aqui, depois do commit; com outbox, o poller publica
            pagamentoEventoProducer.publicarAposCommit(processado.get().evento());

            // atraso configurável para alargar a janela antes do ack; 0 = comportamento natural
            if (atrasoAckMs > 0) {
                try {
                    Thread.sleep(atrasoAckMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            confirmar(ack);
        } finally {
            MDC.remove("pedidoId");
        }
    }

    /** Em modo manual (v1 a v6 sem transações) confirma o offset; com transações o container o confirma. */
    private static void confirmar(Acknowledgment ack) {
        if (ack != null) {
            ack.acknowledge();
        }
    }
}
