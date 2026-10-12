package br.com.gabriel.pedidoeventos.payment_service.pagamento;

import br.com.gabriel.pedidoeventos.payment_service.evento.ItemReservaEvento;
import br.com.gabriel.pedidoeventos.payment_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.idempotencia.DeduplicadorEventos;
import br.com.gabriel.pedidoeventos.payment_service.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PagamentoService {

    private final PagamentoRepository pagamentoRepository;
    private final PagamentoMockService pagamentoMockService;
    private final DeduplicadorEventos deduplicador;
    private final OutboxWriter outboxWriter;

    @Value("${app.topico.pagamentos-processados}")
    private String topicoPagamentosProcessados;

    @Value("${app.demo.produto-envenenado:}")
    private String produtoEnvenenado;

    /**
     * Grava o pagamento, o registro do evento recebido e o evento a publicar (outbox) na mesma
     * transação. Devolve vazio se o evento já tinha sido processado.
     */
    @Transactional
    public Optional<Pagamento> processar(PedidoCriadoEvent evento) {
        if (!deduplicador.registrarSeNovo(evento.eventId())) {
            return Optional.empty();
        }

        // falha persistente simulada (poison message): lança depois do dedupe, então a transação
        // inteira, inclusive o registro do eventId, é desfeita e cada tentativa recomeça limpa
        if (!produtoEnvenenado.isBlank()
                && evento.itens().stream().anyMatch(item -> item.produtoId().toString().equals(produtoEnvenenado))) {
            throw new IllegalStateException("Falha simulada: pedido contém o produto envenenado " + produtoEnvenenado);
        }

        ResultadoPagamento resultado = pagamentoMockService.processar(evento.valorTotal());
        StatusPagamento status = resultado.sucesso() ? StatusPagamento.APROVADO : StatusPagamento.RECUSADO;

        Pagamento pagamento = pagamentoRepository.save(new Pagamento(
                evento.pedidoId(), evento.valorTotal(), status, resultado.motivoRecusa(), Instant.now()));

        PagamentoProcessadoEvent saida = new PagamentoProcessadoEvent(
                UUID.randomUUID(),
                evento.pedidoId(),
                pagamento.getStatus(),
                pagamento.getMotivoRecusa(),
                evento.itens().stream().map(item -> new ItemReservaEvento(item.produtoId(), item.quantidade())).toList(),
                Instant.now()
        );
        outboxWriter.registrar(topicoPagamentosProcessados, evento.pedidoId().toString(), saida.eventId(), saida);

        return Optional.of(pagamento);
    }
}
