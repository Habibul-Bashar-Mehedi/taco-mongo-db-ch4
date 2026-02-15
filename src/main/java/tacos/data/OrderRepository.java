package tacos.data;

import java.util.UUID;

import org.springframework.data.repository.CrudRepository;

import tacos.data.TacoOrder;

public interface OrderRepository
        extends CrudRepository<TacoOrder, UUID> {

}
