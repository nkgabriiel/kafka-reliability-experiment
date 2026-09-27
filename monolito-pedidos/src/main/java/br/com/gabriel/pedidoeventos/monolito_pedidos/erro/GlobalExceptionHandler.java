package br.com.gabriel.pedidoeventos.monolito_pedidos.erro;

import br.com.gabriel.pedidoeventos.monolito_pedidos.estoque.EstoqueInsuficienteException;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento.PagamentoRecusadoException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(PagamentoRecusadoException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public ErroResponse tratarPagamentoRecusado(PagamentoRecusadoException ex) {
        return new ErroResponse("PAGAMENTO_RECUSADO", ex.getMessage());
    }

    @ExceptionHandler(EstoqueInsuficienteException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public ErroResponse tratarEstoqueInsuficiente(EstoqueInsuficienteException ex) {
        return new ErroResponse("ESTOQUE_INSUFICIENTE", ex.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErroResponse tratarValidacao(MethodArgumentNotValidException ex) {
        String mensagem = ex.getBindingResult().getFieldErrors().stream()
                .map(erro -> "%s: %s".formatted(erro.getField(), erro.getDefaultMessage()))
                .collect(Collectors.joining("; "));
        return new ErroResponse("REQUISICAO_INVALIDA", mensagem);
    }
}
