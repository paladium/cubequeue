# Cubequeue
Cubequeue is a simple distributed transaction manager or orchestrator written in GoLang. The idea of distributed transactions are popular within microservices architecture.
To ensure that transactions follow a sequence of steps, transaction manager comes in play.


## Usage

To start using the cubequeue, first import the package into your project:
```go
import (

	"github.com/paladium/cubequeue"
	"github.com/paladium/cubequeue/databases"
	"github.com/paladium/cubequeue/models"
)
```

Next choose one of the available message queues (for now rabbitmq only):
```go
transport, err := cubequeue.NewTransactionTransport(TransactionTransportConnectionSetting{
    URL:   "amqp://guest:guest@localhost:5672",
    Queue: GetDefaultQueueSetting("cubequeue"),
})
```

Next choose one of the available databases, for example mongodb:
```go
database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "cubequeue", "transactions")
```

Finally, create an instance of ```TransactionOrchestrator``` along with available services and transactions:
```go
orchestrator := cubequeue.NewTransactionOrchestrator(&models.TransactionConfig{
    Services: map[string]models.TransactionService{
        "backend": {
            Description: "Main backend",
            Name:        "backend",
            Queue:       "cube-backend",
        },
        "billing": {
            Description: "Billing service",
            Name:        "billing",
            Queue:       "cube-billing",
        },
    },
    Transactions: map[string]models.Transaction{
        "account.create": {
            Description: "Create a new account",
            Stages: []string{
                "backend",
                "billing",
            },
        },
    },
}, transport, database)
```
You can specify as many actions as you need and you can even load them from yaml file, which will be helpful during testing, as you can change the queue names for different environments.

Now, let's run the orchestrator:
```go
orchestrator.Run(cubequeue.RoutingTable{
    "account.create": cubequeue.GetDefaultRoutingHandler(),
}, cubequeue.GetDefaultSubscribeSettings(queue))
```

> Note: the Run function will block the current thread, so if you want to run additional code, run it like this:
```go
go orchestrator.Run(cubequeue.RoutingTable{
    "account.create": cubequeue.GetDefaultRoutingHandler(),
}, cubequeue.GetDefaultSubscribeSettings(queue))
```

## More examples
You can find more examples of using cubequeue in ```examples/``` folder:

- [Starter](./examples/starter/main.go)
- [Custom handler](./examples/custom-handler/main.go)

## License
Apache 2.0

TODO:
- More message queues
- Client library to work with the orchestrator