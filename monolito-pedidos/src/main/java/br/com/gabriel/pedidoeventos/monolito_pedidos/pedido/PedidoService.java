package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido;

import br.com.gabriel.pedidoeventos.monolito_pedidos.estoque.EstoqueService;
import br.com.gabriel.pedidoeventos.monolito_pedidos.estoque.ItemBaixaEstoque;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento.*;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto.PedidoRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class PedidoService {

    private final PedidoRepository pedidoRepository;
    private final PagamentoRepository pagamentoRepository;
    private final PagamentoMockService pagamentoMockService;
    private final EstoqueService estoqueService;

    @Transactional
    public Pedido criarPedido(PedidoRequest request) {
        Pedido pedido = new Pedido();
        request.itens().forEach(itemRequest -> pedido.adicionarItem(
                new ItemPedido(itemRequest.produtoId(), itemRequest.quantidade(), itemRequest.precoUnitario())));
        pedido.calcularValorTotal();
        pedidoRepository.save(pedido);

        ResultadoPagamento resultadoPagamento = pagamentoMockService.processar(pedido.getValorTotal());
        if(!resultadoPagamento.Aprovado()) {
            throw new PagamentoRecusadoException(resultadoPagamento.motivoRecusa());
        }

        Pagamento pagamento = new Pagamento(pedido.getValorTotal(), StatusPagamento.APROVADO, Instant.now());
        pedido.associarPagamento(pagamento);
        pagamentoRepository.save(pagamento);

        List<ItemBaixaEstoque> itensParaBaixa = pedido.getItens().stream()
                .map(item -> new ItemBaixaEstoque(item.getProdutoId(), item.getQuantidade()))
                .toList();
        estoqueService.baixarEstoque(itensParaBaixa);

        pedido.setStatus(StatusPedido.CONCLUIDO);
        return pedido;
    }
}
