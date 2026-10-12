package br.com.gabriel.pedidoeventos.stock_service.estoque;

import br.com.gabriel.pedidoeventos.stock_service.evento.ItemReservaEvento;
import br.com.gabriel.pedidoeventos.stock_service.idempotencia.DeduplicadorEventos;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EstoqueService {

    private final EstoqueRepository estoqueRepository;
    private final DeduplicadorEventos deduplicador;

    /**
     * Reserva o estoque de todos os itens ou de nenhum. Não lança exceção quando falta estoque:
     * a falta é um resultado de negócio, e uma exceção desfaria também o registro do evento
     * como processado.
     */
    @Transactional
    public ResultadoReserva processarReserva(UUID eventId, List<ItemReservaEvento> itens) {
        if (!deduplicador.registrarSeNovo(eventId)) {
            return ResultadoReserva.duplicado();
        }

        Map<UUID, Integer> solicitado = new LinkedHashMap<>();
        for (ItemReservaEvento item : itens) {
            solicitado.merge(item.produtoId(), item.quantidade(), Integer::sum);
        }

        List<Estoque> estoques = new ArrayList<>();
        for (Map.Entry<UUID, Integer> pedido : solicitado.entrySet()) {
            Optional<Estoque> encontrado = estoqueRepository.findByProdutoId(pedido.getKey());
            if (encontrado.isEmpty()) {
                return ResultadoReserva.indisponivel(
                        "Produto %s não encontrado no estoque".formatted(pedido.getKey()));
            }
            Estoque estoque = encontrado.get();
            if (estoque.getQuantidadeDisponivel() < pedido.getValue()) {
                return ResultadoReserva.indisponivel(
                        "Estoque insuficiente para o produto %s (disponível: %d, solicitado: %d)"
                                .formatted(pedido.getKey(), estoque.getQuantidadeDisponivel(), pedido.getValue()));
            }
            estoques.add(estoque);
        }

        for (Estoque estoque : estoques) {
            estoque.setQuantidadeDisponivel(
                    estoque.getQuantidadeDisponivel() - solicitado.get(estoque.getProdutoId()));
        }
        return ResultadoReserva.reservado();
    }
}
