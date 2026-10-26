package br.com.gabriel.pedidoeventos.order_service.kafka;

import br.com.gabriel.pedidoeventos.order_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.order_service.pedido.PedidoService;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * V5b: trata as mensagens de pedidos.criados que esgotaram as tentativas no payment-service e foram para a DLQ,
 * cancelando o pedido. A mensagem na DLQ foi publicada pelo payment-service, com o nome de classe dele no cabeçalho
 * de tipo, então aqui o tipo é fixado e os cabeçalhos de tipo são ignorados.
 */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.protecao.dlq-cancela", havingValue = "true")
public class DlqPedidosListener {

    private final PedidoService pedidoService;

    @KafkaListener(
            topics = "${app.topico.pedidos-criados}.dlq",
            groupId = "${spring.kafka.consumer.group-id}-dlq",
            properties = {
                    "spring.json.use.type.headers=false",
                    "spring.json.value.default.type=br.com.gabriel.pedidoeventos.order_service.evento.PedidoCriadoEvent"})
    public void ouvir(PedidoCriadoEvent evento) {
        MDC.put("pedidoId", evento.pedidoId().toString());
        try {
            pedidoService.cancelarPorFalhaNoProcessamento(evento);
        } finally {
            MDC.remove("pedidoId");
        }
    }
}
