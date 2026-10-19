package br.com.gabriel.pedidoeventos.stock_service.estoque;

import br.com.gabriel.pedidoeventos.stock_service.evento.EstoqueProcessadoEvent;
import br.com.gabriel.pedidoeventos.stock_service.evento.ItemReservaEvento;
import br.com.gabriel.pedidoeventos.stock_service.evento.StatusProcessamentoFinal;
import br.com.gabriel.pedidoeventos.stock_service.idempotencia.DeduplicadorEventos;
import br.com.gabriel.pedidoeventos.stock_service.kafka.EstoqueEventoProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
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
    private final EstoqueEventoProducer estoqueEventoProducer;

    /**
     * Reserva o estoque de todos os itens ou de nenhum e, com a outbox ligada, grava o evento do
     * resultado,
     * tudo na mesma transação do registro do evento recebido. Não lança exceção quando falta
     * estoque: a falta é um resultado de negócio, e uma exceção desfaria também o registro do
     * evento como processado.
     */
    @Transactional
    public ResultadoReserva processarReserva(UUID eventId, UUID pedidoId, List<ItemReservaEvento> itens) {
        if (!deduplicador.registrarSeNovo(eventId)) {
            return ResultadoReserva.duplicado();
        }

        ResultadoReserva resultado = reservar(itens);

        StatusProcessamentoFinal status = resultado.status() == ResultadoReserva.Status.RESERVADO
                ? StatusProcessamentoFinal.ESTOQUE_RESERVADO
                : StatusProcessamentoFinal.ESTOQUE_INDISPONIVEL;
        EstoqueProcessadoEvent saida = new EstoqueProcessadoEvent(
                UUID.randomUUID(), pedidoId, status, resultado.motivo(), Instant.now());
        estoqueEventoProducer.registrarNaTransacao(saida);

        return resultado.comEvento(saida);
    }

    private ResultadoReserva reservar(List<ItemReservaEvento> itens) {
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
