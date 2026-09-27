package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido;

import br.com.gabriel.pedidoeventos.monolito_pedidos.estoque.EstoqueService;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento.PagamentoRepository;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto.ItemPedidoRequest;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto.PedidoRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.willThrow;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
public class PedidoRollbackEmFalhaInesperadaTest {
    @Autowired
    private PedidoService pedidoService;
    @Autowired
    private PedidoRepository pedidoRepository;
    @Autowired
    private PagamentoRepository pagamentoRepository;

    @MockitoBean
    private EstoqueService estoqueService;

    @Test
    void deveFazerRollbackCompleto_quandoOcorreExcecaoInesperadaNoMeioDoFluxo() {
        willThrow(new RuntimeException("Falha técnica simulada (ex: timeout de banco)"))
                .given(estoqueService).baixarEstoque(anyList());

        PedidoRequest request = new PedidoRequest(List.of(
                new ItemPedidoRequest(UUID.randomUUID(), 1, new BigDecimal("50.00"))));

        assertThrows(RuntimeException.class, () -> pedidoService.criarPedido(request));

        assertEquals(0, pedidoRepository.count());
        assertEquals(0, pagamentoRepository.count());
    }
}
