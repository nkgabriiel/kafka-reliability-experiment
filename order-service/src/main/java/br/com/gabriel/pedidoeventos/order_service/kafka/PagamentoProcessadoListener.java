package br.com.gabriel.pedidoeventos.order_service.kafka;

import br.com.gabriel.pedidoeventos.order_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.order_service.pedido.PedidoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PagamentoProcessadoListener {

    private final PedidoService pedidoService;

    @KafkaListener(topics = "${app.topico.pagamentos-processados}", groupId = "${spring.kafka.consumer.group-id}")
    public void ouvir(PagamentoProcessadoEvent evento) {
        log.info("[pedidoId={}] evento recebido de pagamentos.processados: status={}", evento.pedidoId(), evento.status());
        pedidoService.tratarPagamentoRecusado(evento);
    }
}