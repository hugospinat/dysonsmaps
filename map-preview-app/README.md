# Dyson Map Preview App

Local React + TypeScript + Vite app to browse and preview maps from `download_queue_web.json`.

## Data Source

- JSON loaded from `public/data/download_queue_web.json`
- Local images served from `public/data/map_assets/`
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

If the generated queue changes, rebuild the pipeline so `data/outputs/download_queue_web.json` is refreshed:

```powershell
cd ..
.\.venv\Scripts\python.exe scripts\run_pipeline.py --config conf\default.json --stage s04
```
