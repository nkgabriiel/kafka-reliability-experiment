package br.com.gabriel.pedidoeventos.order_service.pedido;

import br.com.gabriel.pedidoeventos.order_service.evento.EstoqueProcessadoEvent;
import br.com.gabriel.pedidoeventos.order_service.evento.ItemPedidoEvento;
import br.com.gabriel.pedidoeventos.order_service.evento.PagamentoProcessadoEvent;
import br.com.gabriel.pedidoeventos.order_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.order_service.evento.StatusPagamento;
import br.com.gabriel.pedidoeventos.order_service.kafka.PedidoEventoProducer;
import br.com.gabriel.pedidoeventos.order_service.pedido.dto.PedidoRequest;
import br.com.gabriel.pedidoeventos.order_service.pedido.dto.PedidoResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoService {

    private static final String MDC_PEDIDO_ID = "pedidoId";

    private final PedidoRepository pedidoRepository;
    private final PedidoEventoProducer pedidoEventoProducer;

    @Transactional
    public Pedido criarPedido(PedidoRequest request) {
        UUID pedidoId = UUID.randomUUID();
        MDC.put(MDC_PEDIDO_ID, pedidoId.toString());
        try {
            log.info("Recebendo novo pedido com {} item(ns)", request.itens().size());

            Pedido pedido = new Pedido();
            pedido.setId(pedidoId);
            request.itens().forEach(itemRequest -> pedido.adicionarItem(
                    new ItemPedido(itemRequest.produtoId(), itemRequest.quantidade(), itemRequest.precoUnitario())));
            pedido.calcularValorTotal();
            pedidoRepository.save(pedido);

            log.info("Pedido persistido como PENDENTE, publicando evento em pedidos.criados");
            pedidoEventoProducer.publicarPedidoCriado(paraEvento(pedido));

            return pedido;
        } finally {
            MDC.remove(MDC_PEDIDO_ID);
        }
    }

    @Transactional(readOnly = true)
    public PedidoResponse buscarPorId(UUID id) {
        Pedido pedido = pedidoRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Pedido %s não encontrado".formatted(id)));
        return PedidoResponse.from(pedido);
    }

    @Transactional
    public void tratarPagamentoRecusado(PagamentoProcessadoEvent evento) {
        if (evento.status() != StatusPagamento.RECUSADO) {
            return;
        }
        Pedido pedido = pedidoRepository.findById(evento.pedidoId())
                .orElseThrow(() -> new NoSuchElementException("Pedido %s não encontrado".formatted(evento.pedidoId())));
        pedido.setStatus(StatusPedido.CANCELADO);
        pedido.setMotivoCancelamento(evento.motivoRecusa());
        log.info("Pedido cancelado por recusa de pagamento: {}", evento.motivoRecusa());
    }

    @Transactional
    public void atualizarStatusFinal(EstoqueProcessadoEvent evento) {
        Pedido pedido = pedidoRepository.findById(evento.pedidoId())
                .orElseThrow(() -> new NoSuchElementException("Pedido %s não encontrado".formatted(evento.pedidoId())));

        switch (evento.status()) {
            case ESTOQUE_RESERVADO -> {
                pedido.setStatus(StatusPedido.CONCLUIDO);
                log.info("Pedido concluído: estoque reservado com sucesso");
            }
            case ESTOQUE_INDISPONIVEL -> {
                pedido.setStatus(StatusPedido.CANCELADO);
                pedido.setMotivoCancelamento(evento.motivo());
                log.info("Pedido cancelado por estoque indisponível: {}", evento.motivo());
            }
        }
    }

    private PedidoCriadoEvent paraEvento(Pedido pedido) {
        return new PedidoCriadoEvent(
                UUID.randomUUID(),
                pedido.getId(),
                pedido.getItens().stream()
                        .map(item -> new ItemPedidoEvento(item.getProdutoId(), item.getQuantidade(), item.getPrecoUnitario()))
                        .toList(),
                pedido.getValorTotal(),
                Instant.now()
        );
    }
}