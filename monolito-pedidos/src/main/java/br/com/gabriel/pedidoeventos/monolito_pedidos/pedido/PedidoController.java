package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido;

import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto.PedidoRequest;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto.PedidoResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/pedidos")
@RequiredArgsConstructor
public class PedidoController {

    private final PedidoService pedidoService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PedidoResponse criarPedido(@Valid @RequestBody PedidoRequest request) {
        Pedido pedido = pedidoService.criarPedido(request);
        return PedidoResponse.from(pedido);
    }
}
