package br.com.gabriel.pedidoeventos.stock_service.kafka;

import br.com.gabriel.pedidoeventos.stock_service.estoque.EstoqueService;
import br.com.gabriel.pedidoeventos.stock_service.estoque.ResultadoReserva;
import br.com.gabriel.pedidoeventos.stock_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.stock_service.evento.StatusPagamento;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PagamentoProcessadoListener {

    private final EstoqueService estoqueService;

    @KafkaListener(topics = "${app.topico.pagamentos-processados}", groupId = "${spring.kafka.consumer.group-id}")
    public void ouvir(PagamentoProcessadoEvent evento) {
        MDC.put("pedidoId", evento.pedidoId().toString());
        try {
            log.info("Evento recebido de pagamentos.processados: status={}", evento.status());

            if (evento.status() == StatusPagamento.RECUSADO) {
                log.info("Pagamento recusado, estoque não tem nada a fazer aqui");
                return;
            }

            ResultadoReserva resultado = estoqueService.processarReserva(
                    evento.eventId(), evento.pedidoId(), evento.itens());
            switch (resultado.status()) {
                case DUPLICADO -> log.warn("Evento {} já processado, descartando sem reprocessar", evento.eventId());
                case RESERVADO -> log.info("Estoque reservado com sucesso, evento gravado na outbox (estoque.processado)");
                case INDISPONIVEL -> log.info("Estoque insuficiente: {}, evento gravado na outbox (estoque.processado)",
                        resultado.motivo());
            }
        } finally {
            MDC.remove("pedidoId");
        }
    }
}
