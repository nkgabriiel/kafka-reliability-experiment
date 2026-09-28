package br.com.gabriel.pedidoeventos.order_service.pedido;


import br.com.gabriel.pedidoeventos.order_service.pedido.dto.PedidoRequest;
import br.com.gabriel.pedidoeventos.order_service.pedido.dto.PedidoResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/pedidos")
@RequiredArgsConstructor
public class PedidoController {

    private final PedidoService pedidoService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PedidoResponse criarPedido(@Valid @RequestBody PedidoRequest request) {
        Pedido pedido = pedidoService.criarPedido(request);
        return PedidoResponse.from(pedido);
    }

    @GetMapping("/{id}")
    public PedidoResponse buscarPorId(@PathVariable UUID id) {
        return pedidoService.buscarPorId(id);
    }
}
