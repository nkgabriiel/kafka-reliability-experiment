package br.com.gabriel.pedidoeventos.payment_service.kafka;

import br.com.gabriel.pedidoeventos.payment_service.evento.PagamentoProcessadoEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PagamentoEventoProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topicoPagamentosProcessados;

    public PagamentoEventoProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                   @Value("${app.topico.pagamentos-processados}") String topicoPagamentosProcessados) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicoPagamentosProcessados = topicoPagamentosProcessados;
    }

    public void publicarPagamentoProcessado(PagamentoProcessadoEvent evento) {
        kafkaTemplate.send(topicoPagamentosProcessados, evento.pedidoId().toString(), evento);
    }
}