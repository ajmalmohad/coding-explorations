from abc import ABC, abstractmethod


class CompressionStrategy(ABC):
    @abstractmethod
    def compress(self, data) -> str:
        pass


class ZipCompressionStrategy(CompressionStrategy):
    def compress(self, data) -> str:
        return f"Data compressed using ZIP: {data}"
    

class RarCompressionStrategy(CompressionStrategy):
    def compress(self, data) -> str:
        return f"Data compressed using RAR: {data}"
    

class GZipCompressionStrategy(CompressionStrategy):
    def compress(self, data) -> str:
        return f"Data compressed using GZIP: {data}"
    

class Compressor:
    def __init__(self, strategy: CompressionStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: CompressionStrategy):
        self._strategy = strategy

    def compress_data(self, data) -> str:
        return self._strategy.compress(data)
    

data = "Example data to be compressed"

compressor = Compressor(ZipCompressionStrategy())
print(compressor.compress_data(data))

compressor.set_strategy(RarCompressionStrategy())
print(compressor.compress_data(data))

compressor.set_strategy(GZipCompressionStrategy())
print(compressor.compress_data(data))
