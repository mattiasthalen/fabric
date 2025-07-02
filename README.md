# Data Warehouse on Microsoft Fabric

This project implements a modern data warehouse on the Microsoft Fabric platform, leveraging a structured, multi-layered approach to data modeling. It uses `dlt` for data ingestion and `SQLMesh` for in-warehouse transformation and modeling, following the Analytical Data Storage (ADS) principles.

## Architecture Overview

The core architecture is organized into three distinct catalogs, analogous to the well-known Bronze, Silver, and Gold layers:

-   **DAS (Data Analytical Storage - Bronze):** This is the landing zone for raw data. Data is extracted from source systems by `dlt` pipelines and loaded here without modification. It is system-oriented and serves as the single source of truth from the perspective of the source systems.

-   **DAB (Data Analytical Business - Silver):** This layer contains business-oriented models. It transforms the raw data from DAS into a cleansed, integrated, and consistent view of the core business concepts (e.g., customers, products, orders). This project uses a "Hook Modelling" methodology for the DAB layer.

-   **DAR (Data Analytical Requirements - Gold):** This layer is designed for specific analytical and reporting requirements. It provides data in a denormalized, aggregated, and user-friendly format, optimized for consumption by BI tools and data scientists. The models in this layer are built using the Unified Star Schema (USS) methodology.

## Technology Stack

-   **Platform:** Microsoft Fabric
-   **Ingestion:** `dlt` (Data Load Tool)
-   **Transformation & Modeling:** `SQLMesh`
-   **Orchestration:** `SQLMesh`

## Modeling Methodologies

### DAB: Hook Modelling

The business-oriented layer (DAB) is built using a metadata-driven approach called "Hook Modelling". This methodology decouples the business logic from the physical SQL code, making the data model more modular, maintainable, and scalable. Instead of writing complex SQL transformations by hand, you define the data model in a series of YAML files, which are then used by a Python blueprint (`sqlmesh/models/dab/hook/hook_blueprint.py`) to generate the final SQL models.

The workflow follows a clear, structured process:

1.  **Define Concepts (`sqlmesh/models/dab/hook/concepts.yaml`):** The first step is to identify and define the core business concepts (e.g., `customer`, `product`). This establishes a consolidated, canonical list of the fundamental business entities.

2.  **Define Key Sets (`sqlmesh/models/dab/hook/keysets.yaml`):** For each concept, you define its corresponding business keys from the source systems. A Key Set creates a globally unique identifier for each distinct business key, linking a concept to a specific source system and qualifier (e.g., `northwind.customer.id`).

3.  **Define Frames and Hooks (`sqlmesh/models/dab/hook/frames.yaml`):** Finally, you map source tables to "Frames" and define "Hooks" to connect source columns to the concepts and keysets. This is where the source data is linked to the business model, creating a clean, integrated view.

### DAR: Unified Star Schema (USS)

The requirements-oriented layer (DAR) is built using the Unified Star Schema (USS) methodology. This is not a traditional star schema with fact and dimension tables. Instead, it provides a semantic layer that leverages the Frames from the DAB layer and connects them through a central "Puppini Bridge" table.

This architecture is composed of:

-   **Frames as Core Components:** The Frames defined in the Hook layer (e.g., for customers, products, orders) are used directly as the building blocks of the analytical schema.
-   **Puppini Bridge for Relationships:** A central bridge table holds all the relationships between the different frames, creating a flexible and scalable model where any frame can be related to any other.

This innovative approach creates a single, unified view of all business concepts and their relationships, allowing analysts to easily query across different business processes without needing to join multiple fact tables. It's a powerful extension of the Hook modelling, providing a seamless path from business definition to analytical consumption.

## Project Structure Highlights

-   `dlt/`: Contains the `dlt` pipelines for ingesting data from various sources (e.g., `northwind`).
-   `sqlmesh/`: Contains the `SQLMesh` project definition.
    -   `models/das/`: Raw data models (Bronze).
    -   `models/dab/`: Business-oriented models and the Hook Modelling configuration (Silver).
    -   `models/dar/`: Requirements-oriented models (Gold).
-   `notebooks/`: Jupyter notebooks for running the code in Fabric.
