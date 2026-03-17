# Weather Agent Notes

The active weather agent configuration for this repository lives in
`weather-agent.toml` and is registered from `.codex/config.toml` under
`[agents.weather-agent]`.

This Markdown file remains as a human-readable companion for the example
workflow:

- The agent preloads the `weather-fetcher` skill
- It fetches current temperature data from Open-Meteo
- It returns temperature, city, and unit to the caller
- It does not write files or create SVG output
