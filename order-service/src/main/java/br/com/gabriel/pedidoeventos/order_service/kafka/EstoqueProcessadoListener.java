package br.com.gabriel.pedidoeventos.order_service.kafka;

import br.com.gabriel.pedidoeventos.order_service.evento.EstoqueProcessadoEvent;
import br.com.gabriel.pedidoeventos.order_service.pedido.PedidoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EstoqueProcessadoListener {

    private final PedidoService pedidoService;

    @KafkaListener(topics = "${app.topico.estoque-processado}", groupId = "${spring.kafka.consumer.group-id}")
    public void ouvir(EstoqueProcessadoEvent evento) {
        log.info("[pedidoId={}] evento recebido de estoque.processado: status={}", evento.pedidoId(), evento.status());
        pedidoService.atualizarStatusFinal(evento);
    }
}
