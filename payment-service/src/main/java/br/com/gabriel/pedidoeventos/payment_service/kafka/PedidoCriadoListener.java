package br.com.gabriel.pedidoeventos.payment_service.kafka;

import br.com.gabriel.pedidoeventos.payment_service.evento.ItemReservaEvento;
import br.com.gabriel.pedidoeventos.payment_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.Pagamento;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.PagamentoMockService;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.PagamentoRepository;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.ResultadoPagamento;
import br.com.gabriel.pedidoeventos.payment_service.pagamento.StatusPagamento;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PedidoCriadoListener {

    @Value("${app.demo.atraso-ack-ms:0}")
    private long atrasoAckMs;

    private final PagamentoRepository pagamentoRepository;
    private final PagamentoMockService pagamentoMockService;
    private final PagamentoEventoProducer pagamentoEventoProducer;

    @KafkaListener(topics = "${app.topico.pedidos-criados}", groupId = "${spring.kafka.consumer.group-id}")
    public void ouvir(PedidoCriadoEvent evento, Acknowledgment ack) {
        MDC.put("pedidoId", evento.pedidoId().toString());
        try {
            log.info("Processando pagamento, valor={}", evento.valorTotal());

            ResultadoPagamento resultado = pagamentoMockService.processar(evento.valorTotal());
            StatusPagamento status = resultado.sucesso() ? StatusPagamento.APROVADO : StatusPagamento.RECUSADO;

            Pagamento pagamento = new Pagamento(
                    evento.pedidoId(), evento.valorTotal(), status, resultado.motivoRecusa(), Instant.now());
            pagamentoRepository.save(pagamento);

            log.info("Pagamento processado: status={}, publicando em pagamentos.processados", status);

            pagamentoEventoProducer.publicarPagamentoProcessado(new PagamentoProcessadoEvent(
                    UUID.randomUUID(),
                    evento.pedidoId(),
                    status,
                    resultado.motivoRecusa(),
                    evento.itens().stream().map(item -> new ItemReservaEvento(item.produtoId(), item.quantidade())).toList(),
                    Instant.now()
            ));

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