# Dyson Map Preview App

Local React + TypeScript + Vite app to browse and preview maps from `download_queue.csv`.

## Data Source

- CSV loaded from `public/data/download_queue.csv`
- Local images served from `public/downloads/` (created as a junction to `../downloads`)
- Remote image URL used as fallback if local image is missing

## Install

```powershell
npm install
```

## Run

```powershell
npm run dev
```

## Build

```powershell
npm run build
```

## Refresh Data

If `download_queue.csv` changes in the repository root, recopy it:

```powershell
Copy-Item ..\download_queue.csv public\data\download_queue.csv -Force
```
