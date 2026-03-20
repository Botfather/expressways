# Expressways Console

Tauri-based monitoring console for the Expressways broker.

## Stack

- React + TypeScript
- Tailwind CSS
- Zustand
- Tauri (Rust backend)

## What It Monitors

- Broker health
- Broker metrics and resilience mode
- Adopter status
- Auth state and revocations
- Discovery registry agent list

## Run

From this directory:

```bash
npm install
npm run dev:tauri
```

The console reads connection settings and a capability token from the UI and calls Expressways directly through Tauri commands.

## Build

```bash
npm run build
npm run build:tauri
```
