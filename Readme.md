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
    A->>A: Create Data    
    A->>B: Save History Data( New Version)
    B->>B: Generate New Version
    B->>C: Sync Data (New Version)
    C->>D: Save History Data (New Version)
    D->>D: Drop More than 2 Version
    D->>C: Sync Data (New Version)
    C->>B: Sync Data (New Version)
    B->>A: Fetch History Data
    B->>A: Rollback Data
    C->>A: Fetch Main Data
    D->>A: Fetch History Main Data
    
```