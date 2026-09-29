#!/usr/bin/env python3

import typing
import abc


def split_pascalcase(name: str) -> str:
    res = "".join([" " + c if c.isupper() else c for c in name])
    return res.strip()


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


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        out_list: list[str] = [x[1] for x in data]
        print(f"{','.join(out_list)}")


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        out_list: list[str] = []
        for elem in data:
            out_list.append(f'"item_{elem[0]}": "{elem[1]}"')
        print(f"{{{', '.join(out_list)}}}")


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
                print("DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self.processor_list) == 0:
            print("No processor found, no data")
        else:
            for proc in self.processor_list:
                total_processed = proc.processing_rank + len(proc.ingested)
                proc_name = split_pascalcase(type(proc).__name__)
                print(f"{proc_name}: total {total_processed} items processed, "
                      f"remaining {len(proc.ingested)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processor_list:
            out_list: list[tuple[int, str]] = []
            for _ in range(min(nb, len(proc.ingested))):
                out_list.append(proc.output())
            if out_list:
                plugin.process_output(out_list)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print()

    print("Registering Processors\n")
    numeric_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    data_stream.register_processor(numeric_proc)
    data_stream.register_processor(text_proc)
    data_stream.register_processor(log_proc)

    batch1 = ['Hello world',
              [3.14, -1, 2.71],
              [{'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'},
               {'log_level': 'INFO',
                'log_message': 'User wil is connected'}],
              42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {batch1}\n")
    data_stream.process_stream(batch1)
    data_stream.print_processors_stats()
    print()
    csv_plugin = CSVPlugin()
    print("Send 3 processed data from each processor to a CSV plugin:")
    data_stream.output_pipeline(3, csv_plugin)
    print()
    data_stream.print_processors_stats()
    print()
    batch2 = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
              [{'log_level': 'ERROR', 'log_message': '500 server crash'},
               {'log_level': 'NOTICE',
                'log_message': 'Certificate expires in 10 days'}],
              [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"Send another batch of data: {batch2}")
    data_stream.process_stream(batch2)
    print()
    data_stream.print_processors_stats()
    print()
    json_plugin = JSONPlugin()
    print("Send 5 processed data from each processor to a JSON plugin:")
    data_stream.output_pipeline(5, json_plugin)
    print()
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
