package br.com.gabriel.pedidoeventos.stock_service.kafka;

import br.com.gabriel.pedidoeventos.stock_service.estoque.EstoqueInsuficienteException;
import br.com.gabriel.pedidoeventos.stock_service.estoque.EstoqueService;
import br.com.gabriel.pedidoeventos.stock_service.evento.EstoqueProcessadoEvent;
import br.com.gabriel.pedidoeventos.stock_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.stock_service.evento.StatusProcessamentoFinal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PagamentoProcessadoListener {

    private final EstoqueService estoqueService;
    private final EstoqueEventoProducer estoqueEventoProducer;

    @KafkaListener(topics = "${app.topico.pagamentos-processados}", groupId = "${spring.kafka.consumer.group-id}")
    public void ouvir(PagamentoProcessadoEvent evento) {
        log.info("[pedidoId={}] evento recebido de pagamentos.processados: status={}", evento.pedidoId(), evento.status());

        if (evento.status() == br.com.gabriel.pedidoeventos.stock_service.evento.StatusPagamento.RECUSADO) {
            publicar(evento.pedidoId(), StatusProcessamentoFinal.PAGAMENTO_RECUSADO, evento.motivoRecusa());
            return;
        }

        try {
            estoqueService.reservarEstoque(evento.itens());
            publicar(evento.pedidoId(), StatusProcessamentoFinal.ESTOQUE_RESERVADO, null);
        } catch (EstoqueInsuficienteException ex) {
            publicar(evento.pedidoId(), StatusProcessamentoFinal.ESTOQUE_INDISPONIVEL, ex.getMessage());
        }
    }

    private void publicar(UUID pedidoId, StatusProcessamentoFinal status, String motivo) {
        estoqueEventoProducer.publicarEstoqueProcessado(
                new EstoqueProcessadoEvent(UUID.randomUUID(), pedidoId, status, motivo, Instant.now()));
    }
}