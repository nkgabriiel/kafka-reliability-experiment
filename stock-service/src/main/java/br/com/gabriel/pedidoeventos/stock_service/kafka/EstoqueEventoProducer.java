package br.com.gabriel.pedidoeventos.stock_service.kafka;

import br.com.gabriel.pedidoeventos.stock_service.evento.EstoqueProcessadoEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class EstoqueEventoProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topicoEstoqueProcessado;

    public EstoqueEventoProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                 @Value("${app.topico.estoque-processado}") String topicoEstoqueProcessado) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicoEstoqueProcessado = topicoEstoqueProcessado;
    }

    public void publicarEstoqueProcessado(EstoqueProcessadoEvent evento) {
        kafkaTemplate.send(topicoEstoqueProcessado, evento.pedidoId().toString(), evento);
    }
}