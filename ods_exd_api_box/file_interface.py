"""Abstract interface for external data file handling."""

from abc import ABC, abstractmethod

from .proto import exd_api


class ExternalDataFileInterface(ABC):
    """Abstract interface for external data file handling."""

    @abstractmethod
    def __init__(self, file_path: str, parameters: str = ""):
        """Initialize the external data file handler.

        Args:
            file_path: Path to the external data file
            parameters: Optional parameters for file handling
        """

    @abstractmethod
    def close(self) -> None:
        """Close the external data file."""

    @abstractmethod
    def fill_structure(self, structure: exd_api.StructureResult) -> None:
        """Fill the structure of the external data file.

        Args:
            structure: StructureResult protobuf message to be populated
        """

    @abstractmethod
    def get_values(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:
        """Get values from the external data file.

        Args:
            request: ValuesRequest protobuf message with query parameters

        Returns:
            ValuesResult protobuf message with the requested values
        """
