#!/usr/bin/env python3

import typing
import abc


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.ingested: list = list()
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

    def ingest(self, data: dict[str:str] | list[dict[str:str]]) -> None:
        if self.validate(data):
            pass
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
        res = num_test.ingest("foo")
        print(f" {res}")
    except ValueError as error:
        print(f" {error}")
    data = [1, 2, 3, 4, 5]
    try:
        if num_test.validate(data):
            print(f" Processing data: {data}")
            num_test.ingest(data)
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
    data = ["Hello", "Nexus", "World"]
    try:
        if text_test.validate(data):
            print(f" Processing data: {data}")
            text_test.ingest(data)
            print(" Extracting 1 value...")
            out = text_test.output()
            print(f" Text value {out[0]}: {out[1]}")
    except ValueError as error:
        print(f" {error}")

    print("\nTesting Log Processor...")


if __name__ == "__main__":
    main()
