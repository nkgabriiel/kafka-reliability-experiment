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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PedidoCriadoListener {

    private final PagamentoRepository pagamentoRepository;
    private final PagamentoMockService pagamentoMockService;
    private final PagamentoEventoProducer pagamentoEventoProducer;

    @KafkaListener(topics = "${app.topico.pedidos-criados}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void ouvir(PedidoCriadoEvent evento) {
        log.info("[pedidoId={}] processando pagamento, valor={}", evento.pedidoId(), evento.valorTotal());

        ResultadoPagamento resultado = pagamentoMockService.processar(evento.valorTotal());
        StatusPagamento status = resultado.sucesso() ? StatusPagamento.APROVADO : StatusPagamento.RECUSADO;

        Pagamento pagamento = new Pagamento(
                evento.pedidoId(), evento.valorTotal(), status, resultado.motivoRecusa(), Instant.now());
        pagamentoRepository.save(pagamento);

        pagamentoEventoProducer.publicarPagamentoProcessado(new PagamentoProcessadoEvent(
                UUID.randomUUID(),
                evento.pedidoId(),
                status,
                resultado.motivoRecusa(),
                evento.itens().stream().map(item -> new ItemReservaEvento(item.produtoId(), item.quantidade())).toList(),
                Instant.now()
        ));
    }
}