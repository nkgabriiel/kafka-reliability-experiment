package br.com.gabriel.pedidoeventos.payment_service.pagamento;

import br.com.gabriel.pedidoeventos.payment_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.payment_service.idempotencia.DeduplicadorEventos;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PagamentoService {

    private final PagamentoRepository pagamentoRepository;
    private final PagamentoMockService pagamentoMockService;
    private final DeduplicadorEventos deduplicador;

    /** Grava o pagamento, ou devolve vazio se o evento já tinha sido processado. */
    @Transactional
    public Optional<Pagamento> processar(PedidoCriadoEvent evento) {
        if (!deduplicador.registrarSeNovo(evento.eventId())) {
            return Optional.empty();
        }

        ResultadoPagamento resultado = pagamentoMockService.processar(evento.valorTotal());
        StatusPagamento status = resultado.sucesso() ? StatusPagamento.APROVADO : StatusPagamento.RECUSADO;

        Pagamento pagamento = new Pagamento(
                evento.pedidoId(), evento.valorTotal(), status, resultado.motivoRecusa(), Instant.now());
        return Optional.of(pagamentoRepository.save(pagamento));
    }
}
