# VendorHub Data Pipeline

Production-like data pipeline for ingesting, processing, and normalizing product data from multiple vendors using PostgreSQL, RabbitMQ, pandas, and SQLAlchemy.

---

## Overview

This project simulates a real-world e-commerce data integration pipeline similar to DK Hardware.

The system is fully deployed on a VPS server and runs continuously via systemd services.

It ingests product data from multiple sources (API, CSV), processes it through RabbitMQ, and stores normalized data for analytics and business use.

---

## Architecture

    Vendors (API / CSV)
        ↓
    Producers
        ↓
    RabbitMQ (Exchange)
        ↓
    Vendor Queues (per vendor)
        ↓
    Consumer (Batch)
        ↓
    PostgreSQL
    ├── staging (raw data)
    ├── staging.rejected_raws (invalid data)
    └── core (normalized data)
        ↓
    Cleanup / Retention Jobs

---

## Key Components

### Producers
- Vendor A (API)
- Vendor B (CSV using pandas)
- Insert raw data into staging
- Publish events to RabbitMQ

### Consumer (Batch Processing)
- Processes messages in batches
- Uses pandas for transformation
- Performs validation and normalization
- Writes valid data to core.products
- Stores invalid rows in staging.rejected_raws
- Tracks price changes in core.product_price_history

### RabbitMQ
- Single exchange: vendor.events
- Separate queues per vendor
- Dead Letter Queues (DLQ) for failed messages

### PostgreSQL

#### staging schema
- vendor_products_raw
- rejected_raws

#### core schema
- products
- product_price_history
- d_vendor, d_currency

---

## Features

- Batch processing with pandas
- Idempotent processing using unique keys
- Message queue decoupling
- Dead Letter Queue support
- Data validation layer (rejected_raws)
- Price history tracking
- VPS deployment with systemd
- Centralized config via .env

---

## Run

docker compose up -d

systemctl start vendorhub-consumer
systemctl start vendorhub-producer-a
systemctl start vendorhub-producer-b

journalctl -u vendorhub-consumer -f

---

## Example Queries

select * from core.products limit 10;

select * from core.product_price_history order by changed_at desc;

---

## Improvements

- Add ClickHouse
- Retry from DLQ
- Data quality dashboards
- Partition tables

---
