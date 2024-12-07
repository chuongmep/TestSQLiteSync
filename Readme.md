Solution Sync Server Data Sample 

- Sync Server Data Sample
- Local Storage 
- Transfer Data 
```mermaid
sequenceDiagram
    participant A as Data
    participant B as History Data
    participant C as Data Server
    participant D as History Data Server
    A->>B: Sync Data
    B->>C: Sync Data
    C->>D: Sync Data
    D->>C: Sync Data
    C->>B: Sync Data
    B->>A: Sync Data
    
```