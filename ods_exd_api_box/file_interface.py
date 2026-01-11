"""Abstract interface for external data file handling."""

from __future__ import annotations

from abc import ABC, abstractmethod

from . import exd_api


class ExdFileInterface(ABC):
    """Abstract interface for external data file handling."""

    @classmethod
    @abstractmethod
    def create(cls, file_path: str, parameters: str) -> ExdFileInterface:
        """Factory method to create a file handler instance.

        Args:
            file_path: Path to the external data file
            parameters: Optional parameters for file handling

        Returns:
            An instance of the file handler
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
