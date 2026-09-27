package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido;

import br.com.gabriel.pedidoeventos.monolito_pedidos.estoque.Estoque;
import br.com.gabriel.pedidoeventos.monolito_pedidos.estoque.EstoqueInsuficienteException;
import br.com.gabriel.pedidoeventos.monolito_pedidos.estoque.EstoqueRepository;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento.PagamentoRecusadoException;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento.PagamentoRepository;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto.ItemPedidoRequest;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto.PedidoRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
public class PedidoIntegrationTest {

    @Autowired
    private PedidoService pedidoService;
    @Autowired
    private PedidoRepository pedidoRepository;
    @Autowired
    private PagamentoRepository pagamentoRepository;
    @Autowired
    private EstoqueRepository estoqueRepository;

    @AfterEach
    void limparBanco() {
        pedidoRepository.deleteAll();
        estoqueRepository.deleteAll();
    }

    @Test
    void deveCriarPedidoEBaixarEstoque_quandoPagamentoAprovadoEEstoqueSuficiente() {
        UUID produtoId = UUID.randomUUID();
        estoqueRepository.save(Estoque.builder()
                .produtoId(produtoId)
                .nomeProduto("Teclado")
                .quantidadeDisponivel(10)
                .build());

        PedidoRequest request = new PedidoRequest(List.of
                (new ItemPedidoRequest(produtoId, 3, new BigDecimal("100.00"))));

        Pedido pedido = pedidoService.criarPedido(request);

        assertEquals(StatusPedido.CONCLUIDO, pedido.getStatus());
        assertEquals(0, new BigDecimal("300.00").compareTo(pedido.getValorTotal()));
        assertEquals(1, pedidoRepository.count());
        assertEquals(1, pagamentoRepository.count());
        assertEquals(7, estoqueRepository.findByProdutoId(produtoId).orElseThrow().getQuantidadeDisponivel());
    }

    @Test
    void naoDevePersistirNada_quandoPagamentoRecusado() {
        UUID produtoId = UUID.randomUUID();
        estoqueRepository.save(Estoque.builder()
                .produtoId(produtoId)
                .nomeProduto("Notebook")
                .quantidadeDisponivel(10)
                .build());

        PedidoRequest request = new PedidoRequest(List.of(
                new ItemPedidoRequest(produtoId, 5, new BigDecimal("5000.00"))));

        assertThrows(PagamentoRecusadoException.class, () -> pedidoService.criarPedido(request));

        assertEquals(0, pedidoRepository.count());
        assertEquals(0, pagamentoRepository.count());
        assertEquals(10, estoqueRepository.findByProdutoId(produtoId).orElseThrow().getQuantidadeDisponivel());
    }

    @Test
    void naoDevePersistirNemBaixarEstoque_quandoEstoqueInsuficiente() {
        UUID produtoId = UUID.randomUUID();
        estoqueRepository.save(Estoque.builder()
                .produtoId(produtoId)
                .nomeProduto("Monitor")
                .quantidadeDisponivel(2)
                .build());

        PedidoRequest request = new PedidoRequest(List.of(
                new ItemPedidoRequest(produtoId, 5, new BigDecimal("100.00"))));

        assertThrows(EstoqueInsuficienteException.class, () -> pedidoService.criarPedido(request));

        assertEquals(0, pedidoRepository.count());
        assertEquals(0, pagamentoRepository.count());
        assertEquals(2, estoqueRepository.findByProdutoId(produtoId).orElseThrow().getQuantidadeDisponivel());
    }
}
