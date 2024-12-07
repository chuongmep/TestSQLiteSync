Solution Sync Server Data Sample 

- Sync Server Data Sample
- Local Storage 
- Transfer Data 
```mermaid
graph TD
    A[Local Storage] --> B[Transfer Data]
    B --> C[Sync Server]
    C --> D[Transfer Data]
    D --> A
```