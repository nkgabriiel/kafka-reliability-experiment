package br.com.gabriel.pedidoeventos.order_service.pedido;

import br.com.gabriel.pedidoeventos.order_service.evento.EstoqueProcessadoEvent;
import br.com.gabriel.pedidoeventos.order_service.evento.ItemPedidoEvento;
import br.com.gabriel.pedidoeventos.order_service.evento.PedidoCriadoEvent;
import br.com.gabriel.pedidoeventos.order_service.kafka.PedidoEventoProducer;
import br.com.gabriel.pedidoeventos.order_service.pedido.dto.PedidoRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PedidoService {

    private final PedidoRepository pedidoRepository;
    private final PedidoEventoProducer pedidoEventoProducer;

    @Transactional
    public Pedido criarPedido(PedidoRequest request) {
        Pedido pedido = new Pedido();
        request.itens().forEach(itemRequest -> pedido.adicionarItem(
                new ItemPedido(itemRequest.produtoId(), itemRequest.quantidade(), itemRequest.precoUnitario())));
        pedido.calcularValorTotal();
        pedidoRepository.save(pedido);

        pedidoEventoProducer.publicarPedidoCriado(paraEvento(pedido));

        return pedido;
    }

    @Transactional(readOnly = true)
    public Pedido buscarPorId(UUID id) {
        return pedidoRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Pedido %s não encontrado".formatted(id)));
    }

    @Transactional
    public void atualizarStatusFinal(EstoqueProcessadoEvent evento) {
        Pedido pedido = pedidoRepository.findById(evento.pedidoId())
                .orElseThrow(() -> new NoSuchElementException("Pedido %s não encontrado".formatted(evento.pedidoId())));

        switch (evento.status()) {
            case ESTOQUE_RESERVADO -> pedido.setStatus(StatusPedido.CONCLUIDO);
            case ESTOQUE_INDISPONIVEL, PAGAMENTO_RECUSADO -> {
                pedido.setStatus(StatusPedido.CANCELADO);
                pedido.setMotivoCancelamento(evento.motivo());
            }
        }
    }

    private PedidoCriadoEvent paraEvento(Pedido pedido) {
        return new PedidoCriadoEvent(UUID.randomUUID(),
                pedido.getId(),
                pedido.getItens().stream()
                        .map(item -> new ItemPedidoEvento(item.getProdutoId(), item.getQuantidade(),
                                item.getPrecoUnitario()))
                        .toList(),
                pedido.getValorTotal(),
                Instant.now());
    }
}
