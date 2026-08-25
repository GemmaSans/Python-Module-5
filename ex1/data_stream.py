#!/usr/bin/env python3

import typing
import abc


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.ingested: list[str] = list()
        self.processing_rank = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        return True

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        current_rank = self.processing_rank
        value = self.ingested.pop(0)
        self.processing_rank += 1
        return (current_rank, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            for i in data:
                if isinstance(i, (int, float)):
                    continue
                else:
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if self.validate(data):
            if isinstance(data, (int, float)):
                self.ingested.append(str(data))
            else:
                for i in data:
                    self.ingested.append(str(i))
        else:
            raise ValueError("Got exception: Improper numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            for i in data:
                if isinstance(i, str):
                    continue
                else:
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            if isinstance(data, str):
                self.ingested.append(data)
            else:
                for i in data:
                    self.ingested.append(i)
        else:
            raise ValueError("Got exception: Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            for x, y in data.items():
                if isinstance(x, str) and isinstance(y, str):
                    continue
                else:
                    return False
            return True
        elif isinstance(data, list):
            for element in data:
                if isinstance(element, dict):
                    for x, y in element.items():
                        if isinstance(x, str) and isinstance(y, str):
                            continue
                        else:
                            return False
                else:
                    return False
            return True
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for element in data:
                    dict_value = ": ".join(element.values())
                    self.ingested.append(dict_value)
            elif isinstance(data, dict):
                dict_value = ": ".join(data.values())
                self.ingested.append(dict_value)
        else:
            raise ValueError("Got exception: Improper log data")


class DataStream:
    def __init__(self) -> None:
        self.processor_list: list[DataProcessor] = list()

    def register_processor(self, proc: DataProcessor) -> None:
        self.processor_list.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            for proc in self.processor_list:
                if proc.validate(element):
                    proc.ingest(element)
                    break
                else:
                    continue
            else:
                print("DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        if len(self.processor_list) == 0:
            print("No processor found, no data\n")
        else:
            print("=== DataStream statistics ===")
            for proc in self.processor_list:
                total_processed = proc.processing_rank + len(proc.ingested)
                print(f"{type(proc).__name__.replace("Proc", " Proc")}: "
                      f"total {total_processed} items processed, "
                      f"remaining {len(proc.ingested)} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("Registering Numeric Processor\n")
    numeric_proc = NumericProcessor()
    data_stream.register_processor(numeric_proc)

    data_for_stream = ['Hello world',
                       [3.14, -1, 2.71],
                       [{'log_level': 'WARNING',
                         'log_message': 'Telnet access! Use ssh instead'},
                        {'log_level': 'INFO',
                        'log_message': 'User wil isconnected'}],
                       42,
                       ['Hi', 'five']]
    print(f"Send first batch of data on stream: {data_for_stream}")
    data_stream.process_stream(data_for_stream)
    data_stream.print_processors_stats()

    print("Registering other data processors\n")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    data_stream.register_processor(text_proc)
    data_stream.register_processor(log_proc)

    print("Send the same batch again")
    data_stream.process_stream(data_for_stream)
    data_stream.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        numeric_proc.output()
    for _ in range(2):
        text_proc.output()
    log_proc.output()
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
