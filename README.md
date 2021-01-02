# Cubequeue
Cubequeue is a simple distributed transaction manager or orchestrator written in GoLang. The idea of distributed transactions are popular within microservices architecture.
To ensure that transactions follow a sequence of steps, transaction manager comes in play.

Cubequeue is easily extensible and does the minimum things to only wrap your messages with transaction concept. After that you are free to customise both the transaction manager (orchestrator) and the client (the microservice itself) to any degree you want.

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
orchestrator.Run(cubequeue.RoutingTable{
    "account.create": cubequeue.GetDefaultRoutingHandler(),
}, cubequeue.GetDefaultSubscribeSettings(queue))
```
If you have other services in your orchestrator, you can run the previous code in its own goroutine:
```go
go orchestrator.Run(cubequeue.RoutingTable{
    "account.create": cubequeue.GetDefaultRoutingHandler(),
}, cubequeue.GetDefaultSubscribeSettings(queue))
```

## More examples
You can find more examples of using cubequeue in ```examples/``` folder:

- [Starter](./examples/starter/main.go)
- [Custom handler](./examples/custom-handler/main.go)


## Client handler
When working with the client, you can either write a custom handler or use the handler provided for you in the client/ folder, which means that you are not locked in to this particular implementation and can bring custom logic. The structure is very similar and the idea is that the client is running a background worker which receives messages either from itself or from transaction service. When it receives the message it processes the message with custom logic.

In case the error happens, the error message is sent to transaction service and transaction services takes care of sending rollback message to every other service for that type of transaction.

First we should create the transport and database as we did before:
```go
transport, err := cubequeue.NewTransactionTransport(TransactionTransportConnectionSetting{
    URL:   "amqp://guest:guest@localhost:5672",
    Queue: cubequeue.GetDefaultQueueSetting("your-service-queue"),
})
database, err := databases.NewTransactionMongoDBDatabase("mongodb://localhost:27017", "your-service-db", "transactions")
```

Next create the background worker:
```go
worker := client.NewBackgroundWorker(transport, database, &client.BackgroundWorkerSettings{
    ServiceName:       "your-service-name",
    TransactionQueue:  "cubequeue",
    SubscribeSettings: cubequeue.GetDefaultSubscribeSettings("your-service-queue"),
})
```
Finally, let's run our worker along with some handlers:
```go
worker.Run(client.TransactionRoutingTable{
    "account.create": client.GetDefaultTransactionRoutingHandler(),
}, client.TransactionRoutingTable{
    "account.create": client.GetDefaultTransactionRoutingHandler(),
})
```
If you have other services in your microservice (like api router), you can run the previous code in its own goroutine:
```go
go worker.Run(client.TransactionRoutingTable{
    "account.create": client.GetDefaultTransactionRoutingHandler(),
}, client.TransactionRoutingTable{
    "account.create": client.GetDefaultTransactionRoutingHandler(),
})
```

The two parameters you specify are: handler for incoming messages and handler for rolling back the transaction. If you choose to omit the rollback, you can simply use the default handler for it, but make sure you provide the key for that message type, otherwise you will get an error.

Finally, if you want to process the first message on your microservice, simply publish it to its own queue:
```go
createInvoice := struct {
    InvoiceNumber string  `json:"invoiceNumber"`
    Filename      string  `json:"filename"`
    Amount        float64 `json:"amount"`
}{
    InvoiceNumber: "34555678",
    Filename:      "invoice-34555678.pdf",
    Amount:        56.67,
}
messageBody, err := json.Marshal(createInvoice)
if err != nil{
    return err
}
transport.Publish("your-service-queue", amqp.Publishing{
    CorrelationId: "unique-id", //Can generate using uuid package
    Type:          "invoice.create", //Type of message
    Body:          messageBody, //Body of your message
})
```

So you can plug this code like the following:
- You receive the API request from user to issue invoice
- You publish a message to your own queue
- The background worker will get the message and process it as a new transaction
- After that it will publish it to transaction service
- The transaction service will take care of publishing it to other services or you can set only one service for that transaction (so that no more services get notified)
- You can now override the custom logic in transaction service to send a Slack message to your channel where you can view the invoice. It is a perfect place for it, as it does nothing to user experience and is useful for debugging and logging.

## License
Apache 2.0

## Contributing
I am welcome to any suggestions on how to improve this transaction service and welcome any contributions to the project.

TODO:
- More message queues