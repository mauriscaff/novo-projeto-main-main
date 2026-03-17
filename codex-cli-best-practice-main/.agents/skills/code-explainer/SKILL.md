---
name: code-explainer
description: Reads and explains a code file, analyzing its role, structure, and key patterns
argument-hint: "[file-path]"
---

# Code Explainer Skill

This skill reads a code file and produces a clear, educational explanation of its purpose, structure, and key patterns.

## Task

Given a file path, read the file and provide a thorough explanation of what the code does, how it works, and the patterns it uses.

## Instructions

### 1. Read the File

Read the file at the provided path. If the file does not exist, report that to the user and stop.

### 2. Identify the File's Role

Determine:
- **Language/Format**: What language or format is the file written in
- **Purpose**: What is the file's primary responsibility in the project
- **Dependencies**: What does it import, require, or depend on
- **Exports**: What does it expose to other parts of the codebase

### 3. Analyze Structure

Break down the file's structure:
- **Top-level organization**: Imports, constants, type definitions, classes, functions
- **Key functions/methods**: What each major function does and why
- **Data flow**: How data moves through the file (inputs, transformations, outputs)
- **Control flow**: Important branching logic, loops, or state management

### 4. Identify Key Patterns

Look for and explain notable patterns:
- **Design patterns**: Factory, observer, strategy, middleware, etc.
- **Architectural patterns**: MVC, pub/sub, pipeline, etc.
- **Language idioms**: Closures, generators, decorators, hooks, etc.
- **Error handling**: How errors are caught, propagated, or recovered from
- **Configuration**: How the code is configured or parameterized

### 5. Produce the Explanation

Structure the explanation as follows:

```markdown
## [Filename]

### Purpose
[1-2 sentence summary of what this file does]

### Role in the Project
[How this file fits into the larger codebase]

### Structure
[Breakdown of the file's organization]

### Key Patterns
[Notable patterns, idioms, or techniques used]

### Dependencies
[What this file depends on and what depends on it]
```

## Notes

- Focus on the "why" behind design decisions, not just the "what"
- Highlight anything unusual or clever in the implementation
- Keep explanations accessible — assume the reader is familiar with the language but not this codebase
- If the file is a configuration file (JSON, YAML, TOML), explain each section and its impact
- If the file is a test file, explain what is being tested and the testing strategy
