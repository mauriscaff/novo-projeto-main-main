# Orchestration Workflow

This document describes the **Skill → Skill** sequential orchestration pattern used in Codex CLI, demonstrated through a weather data fetching and SVG weather card system.

## System Overview

The weather system demonstrates a sequential skill pipeline where one skill feeds data into the next:
- **weather-fetcher**: Fetches real-time temperature data from Open-Meteo API
- **weather-svg-creator**: Takes the temperature data and creates an SVG weather card

This showcases the **Skill → Skill** pipeline pattern, where each skill has a single responsibility and data flows linearly from one to the next.

## Flow Diagram

<p align="center">
  <img src="../!/orchestration-workflow-diagram.svg" alt="Orchestration Workflow: Skill → Skill → Output" width="100%">
</p>

## Component Details

### 1. Weather Fetcher (Skill)

- **Location**: `.agents/skills/weather-fetcher/SKILL.md`
- **Purpose**: Fetch real-time temperature data from Open-Meteo API
- **Input**: City name (or defaults to Dubai, UAE), unit preference (C/F)
- **Output**: Temperature value, unit, and city name
- **API**: Open-Meteo (free, no API key required)

### 2. Weather SVG Creator (Skill)

- **Location**: `.agents/skills/weather-svg-creator/SKILL.md`
- **Purpose**: Create an SVG weather card from temperature data
- **Input**: Temperature value, unit, and city from weather-fetcher
- **Output**: `orchestration-workflow/weather.svg` and `orchestration-workflow/output.md`

## Execution Flow

1. **Invoke weather-fetcher**: Fetch current temperature from Open-Meteo API
2. **Pass data forward**: Temperature, unit, and city flow to the next skill
3. **Invoke weather-svg-creator**: Create SVG weather card
4. **Write output**: SVG card written to `orchestration-workflow/weather.svg` and summary to `orchestration-workflow/output.md`

## Example Execution

```
Input: Fetch weather for Dubai in Celsius
├─ Step 1: weather-fetcher
│  ├─ curl Open-Meteo API (lat=25.2048, lon=55.2708, unit=celsius)
│  ├─ Extract current.temperature_2m → 32
│  └─ Returns: temperature=32, unit=Celsius, city=Dubai
├─ Step 2: weather-svg-creator
│  ├─ Receives: 32°C, Dubai, Celsius
│  ├─ Generates SVG weather card
│  └─ Writes: orchestration-workflow/weather.svg + output.md
└─ Output:
   ├─ orchestration-workflow/weather.svg (SVG card)
   └─ orchestration-workflow/output.md (markdown summary)
```

## Key Design Principles

1. **Sequential Pipeline**: Data flows linearly from fetcher to renderer
2. **Single Responsibility**: Each skill does exactly one thing
3. **Loose Coupling**: Skills communicate via simple data (temperature, unit, city)
4. **No Side Effects in Fetcher**: The fetcher only retrieves data — it does not write files
5. **Idempotent Output**: Running the pipeline again overwrites the previous output cleanly
