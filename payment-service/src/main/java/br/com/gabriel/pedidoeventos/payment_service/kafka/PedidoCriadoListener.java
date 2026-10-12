package br.com.gabriel.pedidoeventos.payment_service.kafka;

import br.com.gabriel.pedidoeventos.payment_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.Pagamento;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.PagamentoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PedidoCriadoListener {

    @Value("${app.demo.atraso-ack-ms:0}")
    private long atrasoAckMs;

    private final PagamentoService pagamentoService;

    @KafkaListener(topics = "${app.topico.pedidos-criados}", groupId = "${spring.kafka.consumer.group-id}")
    public void ouvir(PedidoCriadoEvent evento, Acknowledgment ack) {
        MDC.put("pedidoId", evento.pedidoId().toString());
        try {
            log.info("Processando pagamento, valor={}", evento.valorTotal());

            Optional<Pagamento> processado = pagamentoService.processar(evento);
            if (processado.isEmpty()) {
                log.warn("Evento {} já processado, descartando sem reprocessar", evento.eventId());
                ack.acknowledge();
                return;
            }

            log.info("Pagamento processado: status={}, evento gravado na outbox (pagamentos.processados)",
                    processado.get().getStatus());

            // atraso configurável para alargar a janela antes do ack; 0 = comportamento natural
            if (atrasoAckMs > 0) {
                try {
                    Thread.sleep(atrasoAckMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            ack.acknowledge();
        } finally {
            MDC.remove("pedidoId");
        }
    }
}
