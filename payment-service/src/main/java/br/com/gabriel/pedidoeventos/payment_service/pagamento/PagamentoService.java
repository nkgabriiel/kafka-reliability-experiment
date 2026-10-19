package br.com.gabriel.pedidoeventos.payment_service.pagamento;

import br.com.gabriel.pedidoeventos.payment_service.evento.ItemReservaEvento;
import br.com.gabriel.pedidoeventos.payment_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.idempotencia.DeduplicadorEventos;
import br.com.gabriel.pedidoeventos.payment_service.kafka.PagamentoEventoProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
public class PagamentoService {

    private final PagamentoRepository pagamentoRepository;
    private final PagamentoMockService pagamentoMockService;
    private final DeduplicadorEventos deduplicador;
    private final PagamentoEventoProducer pagamentoEventoProducer;

    // falha aleatória simulada: percentual (0 a 100) das chamadas que lançam exceção; 0 = desligado
    @Value("${app.demo.taxa-falha:0}")
    private double taxaFalhaPercentual;

    @Value("${app.demo.produto-envenenado:}")
    private String produtoEnvenenado;

    /**
     * Grava o pagamento e o registro do evento recebido (e, com a outbox ligada, o evento a publicar) na mesma
     * transação. Devolve vazio se o evento já tinha sido processado.
     */
    @Transactional
    public Optional<ProcessamentoPagamento> processar(PedidoCriadoEvent evento) {
        if (!deduplicador.registrarSeNovo(evento.eventId())) {
            return Optional.empty();
        }

        // falha persistente simulada (poison message): lança depois do dedupe, então a transação
        // inteira, inclusive o registro do eventId, é desfeita e cada tentativa recomeça limpa
        if (!produtoEnvenenado.isBlank()
                && evento.itens().stream().anyMatch(item -> item.produtoId().toString().equals(produtoEnvenenado))) {
            throw new IllegalStateException("Falha simulada: pedido contém o produto envenenado " + produtoEnvenenado);
        }

        // falha transitória simulada (FAILURE_RATE): cada chamada sorteia de novo, então uma nova tentativa pode passar
        if (taxaFalhaPercentual > 0 && ThreadLocalRandom.current().nextDouble() * 100 < taxaFalhaPercentual) {
            throw new IllegalStateException("Falha aleatória simulada (FAILURE_RATE=" + taxaFalhaPercentual + "%)");
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
        pagamentoEventoProducer.registrarNaTransacao(saida);

        return Optional.of(new ProcessamentoPagamento(pagamento, saida));
    }
}
