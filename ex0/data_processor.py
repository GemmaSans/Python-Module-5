#!/usr/bin/env python3

import typing
import abc


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.ingested: list[str] = []
        self.processing_rank: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.ingested:
            raise IndexError("Got exception: No data available to output")
        current_rank = self.processing_rank
        value = self.ingested.pop(0)
        self.processing_rank += 1
        return (current_rank, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, list):
            return all(isinstance(i, (int, float)) for i in data)
        return isinstance(data, (int, float))

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
        if isinstance(data, list):
            return all(isinstance(i, str) for i in data)
        return isinstance(data, str)

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
            return all(isinstance(k, str) and isinstance(v, str)
                       for k, v in data.items())
        if isinstance(data, list):
            for element in data:
                if not isinstance(element, dict):
                    return False
                if not all(isinstance(k, str) and isinstance(v, str)
                           for k, v in element.items()):
                    return False
            return True
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


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    num_test = NumericProcessor()
    res = num_test.validate(42)
    print(f" Trying to validate input '42': {res}")
    res = num_test.validate("Hello")
    print(f" Trying to validate input 'Hello': {res}")
    try:
        print(" Test invalid ingestion of string 'foo' "
              "without prior validation:")
        num_test.ingest("foo")  # type: ignore
    except ValueError as error:
        print(f" {error}")
    num_data: list[int | float] = [1, 2, 3, 4, 5]
    try:
        if num_test.validate(num_data):
            print(f" Processing data: {num_data}")
            num_test.ingest(num_data)
            print(" Extracting 3 values...")
            for _ in range(3):
                out = num_test.output()
                print(f" Numeric value {out[0]}: {out[1]}")
    except ValueError as error:
        print(f" {error}")

    print("\nTesting Text Processor...")
    text_test = TextProcessor()
    res = text_test.validate(42)
    print(f" Trying to validate input '42': {res}")
    text_data = ["Hello", "Nexus", "World"]
    try:
        if text_test.validate(text_data):
            print(f" Processing data: {text_data}")
            text_test.ingest(text_data)
            print(" Extracting 1 value...")
            out = text_test.output()
            print(f" Text value {out[0]}: {out[1]}")
    except ValueError as error:
        print(f" {error}")

    print("\nTesting Log Processor...")
    log_test = LogProcessor()
    res = log_test.validate("Hello")
    print(f" Trying to validate input 'Hello': {res}")
    log_data = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    try:
        if log_test.validate(log_data):
            print(f" Processing data: {log_data}")
            log_test.ingest(log_data)
            print(" Extracting 2 values...")
            for _ in range(2):
                out = log_test.output()
                print(f" Log entry {out[0]}: {out[1]}")
    except ValueError as error:
        print(f" {error}")


if __name__ == "__main__":
    main()
