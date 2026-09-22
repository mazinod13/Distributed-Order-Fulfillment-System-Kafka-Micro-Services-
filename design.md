How an order moves through the system:

Happy path:
 Customer
    │ place order
    ▼
┌─────────┐ OrderCreated ┌───────────┐ InventoryReserved ┌─────────┐ PaymentCompleted ┌──────────┐
│  Order  │─────────────▶│ Inventory │──────────────────▶│ Payment │─────────────────▶│ Shipping │
│ service │              │  service  │                   │ service │                  │ service  │
└─────────┘              └───────────┘                   └─────────┘                  └──────────┘
     ▲                                                                                     │
     └──────────────────────── OrderShipped ───────────────────────────────────────────────┘
          (the order service listens to every event and updates the order's status)


Failure paths:
Out of stock:   OrderCreated → InventoryRejected                          → order CANCELLED
Payment fails:  OrderCreated → InventoryReserved → PaymentFailed
                → Inventory releases the stock (InventoryReleased)         → order CANCELLED

