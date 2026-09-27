package br.com.gabriel.pedidoeventos.order_service.kafka;

import br.com.gabriel.pedidoeventos.order_service.evento.PedidoCriadoEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PedidoEventoProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topicoPedidosCriados;

    public PedidoEventoProducer(KafkaTemplate<String, Object> kafkaTemplate, @Value("${app.topico.pedidos-criados}") String topicoPedidosCriados) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicoPedidosCriados = topicoPedidosCriados;
    }

    public void publicarPedidoCriado(PedidoCriadoEvent evento) {
        kafkaTemplate.send(topicoPedidosCriados, evento.pedidoId().toString(), evento);
    }
}
