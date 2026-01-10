"""Registry for external data file handlers."""
from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Callable

from . import ExdFileInterface


@dataclass
class FileType:
    """Data class representing a file type and its associated extension patterns."""

    name: str
    file_patterns: list[str]
    factory: Callable[[str, str], ExdFileInterface]


class FileHandlerRegistry:
    """Registry for managing external data file handlers.

    This registry allows registering different file handler implementations
    and creating instances based on file type or extension.
    """

    _handlers: dict[str, FileType] = {}

    @classmethod
    def register(cls,
                 file_type_name: str,
                 factory: Callable[[str, str], ExdFileInterface],
                 file_patterns: list[str] = None
                 ) -> None:
        """Register a file handler factory for a specific file type.

        Args:
            file_type: File type identifier (e.g., 'tdms', 'hdf5')
            factory: Callable that creates ExternalDataFileInterface instances
        """
        cls._handlers[file_type_name] = FileType(
            file_type_name, file_patterns if file_patterns is not None else [], factory)

    @classmethod
    def create(cls, file_type_name: str, file_path: str, parameters: str) -> ExdFileInterface:
        """Create an instance of the registered handler for the given file type.

        Args:
            file_type_name: File type identifier
            file_path: Path to the external data file
            parameters: Optional parameters for file handling

        Returns:
            ExternalDataFileInterface implementation

        Raises:
            ValueError: If file type is not registered
        """
        if not cls._handlers:
            raise ValueError("No file handlers registered")
        file_type = cls._handlers.get(file_type_name)
        if file_type is None:
            raise ValueError(f"File type '{file_type_name}' is not registered")
        if file_type.factory is None:
            raise ValueError(
                f"No factory registered for file type '{file_type_name}'")
        return file_type.factory(file_path, parameters)

    @classmethod
    def get_file_type_name(cls, file_path: str) -> str:
        """Determine file type from file extension.

      Args:
          file_path: Path to the file

      Returns:
          File type based on extension

      Raises:
          ValueError: If no matching handler is found for the file extension
      """
        if not cls._handlers:
            raise ValueError("No file handlers registered")
        if len(cls._handlers) == 1:
            return next(iter(cls._handlers.keys()))

        # Extract filename using Path
        filename = Path(file_path).name.upper()

        # Match filename against registered file_patterns using fnmatch
        for file_type_name, file_type_info in cls._handlers.items():
            for pattern in file_type_info.file_patterns:
                if fnmatch(filename, pattern.upper()):
                    return file_type_name

        raise ValueError(
            f"No handler registered for file pattern matching '{filename}'")

    @classmethod
    def create_from_path(cls, file_path: str, parameters: str) -> ExdFileInterface:
        """Create an instance by automatically detecting file type from path.

        Args:
            file_path: Path to the external data file
            parameters: Optional parameters for file handling

        Returns:
            ExternalDataFileInterface implementation

        Raises:
            ValueError: If file type cannot be detected or is not registered
        """
        file_type_name = cls.get_file_type_name(file_path)
        return cls.create(file_type_name, file_path, parameters)
