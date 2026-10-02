
```python
poetry install
poetry run main
```


```python
poetry run main <path_to_target_file> --scenario=<GAUSSIAN, EXPONENTIAL, LINEAR, CONSTANT_RATE>
```
## Example
```python
poetry run main ../beam-applications-java/data/riot_events_TAXI.csv --scenario=EXPONENTIAL

poetry run main ../beam-applications-java/data/riot_events_TAXI.csv --scenario=CONSTANT_RATE --events-per-second=800
```
