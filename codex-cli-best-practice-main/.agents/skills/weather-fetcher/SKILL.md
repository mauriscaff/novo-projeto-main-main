---
name: weather-fetcher
description: Instructions for fetching current weather temperature data from Open-Meteo API for a given city
---

# Weather Fetcher Skill

This skill provides instructions for fetching current weather data from the Open-Meteo API.

## Task

Fetch the current temperature for a given city using coordinates and the requested unit (Celsius or Fahrenheit).

## Instructions

1. **Determine Coordinates**: Identify the latitude and longitude for the requested city. If no city is specified, default to Dubai, UAE (latitude: 25.2048, longitude: 55.2708).

2. **Fetch Weather Data**: Use `curl` to hit the Open-Meteo API with the appropriate parameters.

   For **Celsius**:
   ```bash
   curl -s "https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current=temperature_2m&temperature_unit=celsius"
   ```

   For **Fahrenheit**:
   ```bash
   curl -s "https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current=temperature_2m&temperature_unit=fahrenheit"
   ```

   Replace `{LAT}` and `{LON}` with the city's coordinates.

3. **Extract Temperature**: From the JSON response, extract the current temperature:
   - Field: `current.temperature_2m`
   - Unit label: `current_units.temperature_2m`

   Example using jq:
   ```bash
   curl -s "https://api.open-meteo.com/v1/forecast?latitude=25.2048&longitude=55.2708&current=temperature_2m&temperature_unit=celsius" | jq '.current.temperature_2m'
   ```

4. **Return Result**: Return the temperature value and unit clearly.

## Expected Output

After completing this skill's instructions:
```
Current Temperature: [X]°[C/F]
City: [City Name]
Unit: [Celsius/Fahrenheit]
```

## Common City Coordinates

| City       | Latitude | Longitude |
|------------|----------|-----------|
| Dubai      | 25.2048  | 55.2708   |
| London     | 51.5074  | -0.1278   |
| New York   | 40.7128  | -74.0060  |
| Tokyo      | 35.6762  | 139.6503  |
| Sydney     | -33.8688 | 151.2093  |

## Notes

- Only fetch the temperature — do not perform any transformations or write any files
- Open-Meteo is free, requires no API key, and uses coordinate-based lookups
- Return the numeric temperature value and unit clearly
- Support both Celsius and Fahrenheit based on the caller's request
